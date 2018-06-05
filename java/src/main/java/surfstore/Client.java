package surfstore;

import java.util.*;
import java.nio.charset.StandardCharsets;
import java.io.File;
import java.io.FileWriter;
import java.io.BufferedWriter;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.io.IOException;
import java.io.EOFException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import com.google.protobuf.ByteString;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import surfstore.SurfStoreBasic.*;

public final class Client {
    private static final Logger logger = Logger.getLogger(Client.class.getName());

    private final ManagedChannel metadataChannel;
    private final MetadataStoreGrpc.MetadataStoreBlockingStub metadataStub;

    private final ManagedChannel blockChannel;
    private final BlockStoreGrpc.BlockStoreBlockingStub blockStub;

    private final ConfigReader config;

    public Client(ConfigReader config) {
        this.metadataChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getMetadataPort(config.getLeaderNum()))
                .usePlaintext(true).build();
        this.metadataStub = MetadataStoreGrpc.newBlockingStub(metadataChannel);

        this.blockChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getBlockPort())
                .usePlaintext(true).build();
        this.blockStub = BlockStoreGrpc.newBlockingStub(blockChannel);

        this.config = config;
    }

    public void shutdown() throws InterruptedException {
        metadataChannel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        blockChannel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    private void ensure(boolean b){
	if (b == false){
		throw new RuntimeException("Assertion failed!");
	}
    } 

    private static Block stringToBlock(String s){
	Block.Builder builder = Block.newBuilder();
	try{
	builder.setData(ByteString.copyFrom(s, "UTF-8"));
	} catch (UnsupportedEncodingException e){
		throw new RuntimeException(e);
	}
	builder.setHash(HashUtils.sha256(s.getBytes(StandardCharsets.UTF_8)));

	return builder.build();
    }

   // private void go() {
   //     metadataStub.ping(Empty.newBuilder().build());
   //     logger.info("Successfully pinged the Metadata server");

   //     blockStub.ping(Empty.newBuilder().build());
   //     logger.info("Successfully pinged the Blockstore server");
   //      TODO: Implement your client here
   //     Block b1 = stringToBlock("block_01");
   //     Block b2 = stringToBlock("block_02");

   //     ensure(blockStub.hasBlock(b1).getAnswer() == false);
   //     ensure(blockStub.hasBlock(b2).getAnswer() == false);

   //     blockStub.storeBlock(b1);
   //     ensure(blockStub.hasBlock(b1).getAnswer() == true);

   //     blockStub.storeBlock(b2);
   //     ensure(blockStub.hasBlock(b2).getAnswer() == true);

   //     Block b1prime = blockStub.getBlock(b1);
   //     ensure(b1prime.getHash().equals(b1.getHash()));
   //     ensure(b1.getData().equals(b1.getData()));

   //     logger.info("We passed all the test!");
   // }

    private void upload(String targetf) throws IOException, RuntimeException{
	ArrayList<String> hashList = new ArrayList();
	LinkedHashMap<String, ByteString> hashToPlain = new LinkedHashMap();
	String[] nameList = targetf.split("/");
	String realName = nameList[nameList.length-1];
	try(RandomAccessFile file = new RandomAccessFile(targetf, "r")){
		byte[] tmp;
		for(long i = 0; i < file.length() / 4096 + 1; i++){
			if(i < file.length() / 4096) 
				tmp = new byte[4096];
			else
				tmp = new byte[(int)file.length() % 4096];
			file.readFully(tmp);
			ByteString data = ByteString.copyFrom(tmp);
			String hash = HashUtils.sha256(tmp); 
			hashList.add(hash);
			hashToPlain.put(hash, data);
		}
	}catch (IOException e){
		throw e;
	}
	
	FileInfo reqFileInfo = FileInfo.newBuilder().setFilename(realName).build();
	FileInfo respFileInfo = metadataStub.readFile(reqFileInfo);
	int version = respFileInfo.getVersion();
	
	reqFileInfo = FileInfo.newBuilder().setFilename(realName)
		.setVersion(version+1)
		.addAllBlocklist(hashList)
		.build();
	WriteResult result = metadataStub.modifyFile(reqFileInfo);
	ArrayList<String> missList = new ArrayList(result.getMissingBlocksList()); 
	//Check if needed to connect to blockStore
	if(missList.size() != 0){
		Block.Builder blBuilder = Block.newBuilder();	
		for(String missHash : missList){
			blockStub.storeBlock(blBuilder.setHash(missHash)
					.setData(hashToPlain.get(missHash))
					.build());
		}

		reqFileInfo = FileInfo.newBuilder().setFilename(realName)
			.setVersion(version+1)
			.addAllBlocklist(hashList)
			.build();
		result = metadataStub.modifyFile(reqFileInfo);
	}
	if(result.getResult() == WriteResult.Result.OK){
		System.out.println("OK");
		return;
	}
	else
		throw new RuntimeException("Metadata blocks checking");

    }

    private void download(String targetf, String tfolder) throws IOException{
	ArrayList<String> hashList = new ArrayList();
	LinkedHashMap<String, ByteString> hashToPlain = new LinkedHashMap();

	FileInfo reqFileInfo = FileInfo.newBuilder().setFilename(targetf).build();
	FileInfo respFileInfo = metadataStub.readFile(reqFileInfo);

	if(respFileInfo.getVersion() == 0 || respFileInfo.getBlocklist(0).equals("0")){
		System.out.println("Not Found"); 	
		return;
	}
	ArrayList<String> pulledHashList = new ArrayList(respFileInfo.getBlocklistList());
	
	String content = new String();
	BufferedWriter writer = null;
	try(RandomAccessFile file = new RandomAccessFile(tfolder + "/" + targetf, "rw")){
		byte[] tmp;
		for(long i = 0; i < file.length() / 4096; i++){
			if(i < file.length() / 4096) 
				tmp = new byte[4096];
			else
				tmp = new byte[(int)file.length() % 4096];
			file.readFully(tmp);
			ByteString data = ByteString.copyFrom(tmp);
			String hash = HashUtils.sha256(tmp); 
			hashList.add(hash);
			hashToPlain.put(hash, data);
		}
		//for(String hash : hashToPlain.keySet()){
		//	System.out.println(new String(hashToPlain.get(hash).toByteArray()));	
		//}
		for(String pulledHash : pulledHashList){
			if(hashToPlain.containsKey(pulledHash)){
				content += new String(hashToPlain.get(pulledHash).toByteArray());
			} else {
				Block block = blockStub.getBlock(Block.newBuilder()
						.setHash(pulledHash)
						.build());
				content += new String(block.getData().toByteArray());
			}
		}
	}catch (IOException e){
		throw e;
	}
	try{
		writer = new BufferedWriter( new FileWriter(tfolder + "/" + targetf));
		writer.write(content);
		writer.close();
		System.out.println("OK");
	}catch (IOException e){
		throw e;
	}
		return;
    }

    private int getVersion(String targetf){
	FileInfo respFileInfo = metadataStub.readFile(FileInfo.newBuilder()
							.setFilename(targetf)
							.build());
	return respFileInfo.getVersion();
    }

    private void delete(String targetf){
	WriteResult result = metadataStub.deleteFile(FileInfo.newBuilder()
							.setFilename(targetf)
							.build());
	if(result.getResult() == WriteResult.Result.OK)
		System.out.println("OK");
	else
		System.out.println("Not Found");
	return;
    }

    private static Namespace parseArgs(String[] args) {
        ArgumentParser parser = ArgumentParsers.newFor("Client").build()
                .description("Client for SurfStore");
        parser.addArgument("config_file").type(String.class)
                .help("Path to configuration file");
        parser.addArgument("operation").type(String.class)
                .help("Operation: upload/download/delete/getversion");
        parser.addArgument("target_file").type(String.class)
                .help("Path to target file");
        parser.addArgument("target_folder").nargs("?").type(String.class)
                .help("Path to target folder");
        
        Namespace res = null;
        try {
            res = parser.parseArgs(args);
        } catch (ArgumentParserException e){
            parser.handleError(e);
        }
        return res;
    }

    public static void main(String[] args) throws Exception {
        Namespace c_args = parseArgs(args);
        if (c_args == null){
            throw new RuntimeException("Argument parsing failed");
        }

        File configf = new File(c_args.getString("config_file"));
        String targetf = c_args.getString("target_file");
        ConfigReader config = new ConfigReader(configf);

        Client client = new Client(config);
        
        try {
		if(c_args.getString("operation").equals("upload"))
			client.upload(targetf);
		if(c_args.getString("operation").equals("download")){
			String tfolder = c_args.getString("target_folder");
			client.download(targetf, tfolder);
		}
		if(c_args.getString("operation").equals("getversion"))
			System.out.println(client.getVersion(targetf));
		if(c_args.getString("operation").equals("delete"))
			client.delete(targetf);
        } 
        catch (IOException e){
        	System.err.println("Failed in reading " + targetf);}
        finally {
            client.shutdown();
        }
    }

}
