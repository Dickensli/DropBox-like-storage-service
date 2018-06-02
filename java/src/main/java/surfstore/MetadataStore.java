package surfstore;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.logging.Logger;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import surfstore.SurfStoreBasic.*;

public final class MetadataStore {
    private static final Logger logger = Logger.getLogger(MetadataStore.class.getName());

    private final ManagedChannel blockChannel;
    private static BlockStoreGrpc.BlockStoreBlockingStub blockStub;

    protected Server server;
    private static ConfigReader config;
    private static boolean leader;
    private static boolean crashed;

    public MetadataStore(ConfigReader config, Namespace c_args) {
    	this.blockChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getBlockPort())
                .usePlaintext(true).build();
        this.blockStub = BlockStoreGrpc.newBlockingStub(blockChannel);

	this.config = config;
	this.leader = config.getLeaderNum() == c_args.getInt("number");
	this.crashed = false;
	}

	private void start(int port, int numThreads) throws IOException {
        server = ServerBuilder.forPort(port)
                .addService(new MetadataStoreImpl())
                .executor(Executors.newFixedThreadPool(numThreads))
                .build()
                .start();
        logger.info("Server started, listening on " + port);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                MetadataStore.this.stop();
                System.err.println("*** server shut down");
            }
        });
    }

    private void stop() {
        if (server != null) {
            server.shutdown();
        }
    }

    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    private static Namespace parseArgs(String[] args) {
        ArgumentParser parser = ArgumentParsers.newFor("MetadataStore").build()
                .description("MetadataStore server for SurfStore");
        parser.addArgument("config_file").type(String.class)
                .help("Path to configuration file");
        parser.addArgument("-n", "--number").type(Integer.class).setDefault(1)
                .help("Set which number this server is");
        parser.addArgument("-t", "--threads").type(Integer.class).setDefault(10)
                .help("Maximum number of concurrent threads");

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
        ConfigReader config = new ConfigReader(configf);

        if (c_args.getInt("number") > config.getNumMetadataServers()) {
            throw new RuntimeException(String.format("metadata%d not in config file", c_args.getInt("number")));
        }

        final MetadataStore server = new MetadataStore(config, c_args);
        server.start(config.getMetadataPort(c_args.getInt("number")), c_args.getInt("threads"));
        server.blockUntilShutdown();
    }

    static class MetadataStoreImpl extends MetadataStoreGrpc.MetadataStoreImplBase {
	protected Map<String, ArrayList<String>> metadataMap;
	protected Map<String, Integer> versionMap;
	
	public MetadataStoreImpl(){
		super();
		this.metadataMap = new HashMap<String, ArrayList<String>>();
		this.versionMap = new HashMap<String, Integer>();
	}

        @Override
        public void ping(Empty req, final StreamObserver<Empty> responseObserver) {
            Empty response = Empty.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

	@Override
	public void readFile(surfstore.SurfStoreBasic.FileInfo request,
        	io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.FileInfo> responseObserver) {
		logger.info("Reading file  " + request.getFilename());
		
		ArrayList<String> hashlist;
		Iterable<String> iter_hashlist;
		int version;

		if (metadataMap.containsKey(request.getFilename())) {
			hashlist = metadataMap.get(request.getFilename());
			version = versionMap.get(request.getFilename());
		} else {
			hashlist = new ArrayList<String>();
			version = 0; 
		}
		iter_hashlist = hashlist;

		FileInfo.Builder builder = FileInfo.newBuilder();
		builder.setVersion(version);
		builder.addAllBlocklist(iter_hashlist); 
		FileInfo response = builder.build();
		
		responseObserver.onNext(response);
		responseObserver.onCompleted();
	}

	private ArrayList<String> findBlock (ArrayList<String> hashlist) {
		blockStub.ping(Empty.newBuilder().build());
		logger.info("Successfully pinged the Blockstore server from MetadataStore");
		
		ArrayList<String> misslist = new ArrayList<String>();
	
		for (int i = 0; i < hashlist.size(); i++){
			Block.Builder builder = Block.newBuilder();
			builder.setHash(hashlist.get(i));
			Block checkblock = builder.build();
			if (blockStub.hasBlock(checkblock).getAnswer() == false) {
				misslist.add(hashlist.get(i));
			}
		}
		return misslist;
	}

	@Override
    	public void modifyFile(surfstore.SurfStoreBasic.FileInfo request,
        	io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.WriteResult> responseObserver) {
		
		WriteResult.Builder builder = WriteResult.newBuilder();
		if (leader){ // is leader
			logger.info("Modifying file  " + request.getFilename());

                	int cur_version = versionMap.get(request.getFilename());
                	ArrayList<String> hashlist = new ArrayList<String>(request.getBlocklistList()); 
               		ArrayList<String> misslist;
                	Iterable<String> iter_misslist;
			if (request.getVersion() == cur_version + 1) { /* valid */
				// check missing blocks
				misslist = findBlock(hashlist);
				
				if (!misslist.isEmpty()){
					iter_misslist = misslist;
					builder.setResult(WriteResult.Result.MISSING_BLOCKS);
                			builder.setCurrentVersion(cur_version);
					builder.addAllMissingBlocks(iter_misslist); /* check if addAll repeated field workds */
				} else { // success
					// update maps if no missing block
					versionMap.put(request.getFilename(),  request.getVersion());
                        		metadataMap.put(request.getFilename(), hashlist);
					builder.setResult(WriteResult.Result.OK);
					builder.setCurrentVersion(request.getVersion());
				}
			} else { /* not valid */
				builder.setResult(WriteResult.Result.OLD_VERSION);
				builder.setCurrentVersion(cur_version); // check if CurrentVersion works
			}
		} else {
			builder.setResult(WriteResult.Result.NOT_LEADER);
		}

		WriteResult response = builder.build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
    		
	}

	@Override
    	public void deleteFile(surfstore.SurfStoreBasic.FileInfo request,
        	io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.WriteResult> responseObserver) {

		WriteResult.Builder builder = WriteResult.newBuilder();
		if (leader) {
			logger.info("Deleting file  " + request.getFilename());

                	int cur_version = versionMap.get(request.getFilename());
			if (request.getVersion() == cur_version + 1) { /* valid */
				// update maps
				ArrayList<String> hashlist = new ArrayList<String>();
				versionMap.put(request.getFilename(), request.getVersion());
				metadataMap.put(request.getFilename(), hashlist);
				
				builder.setResult(WriteResult.Result.OK);
				builder.setCurrentVersion(request.getVersion());
			} else { /* not valid */
                        	builder.setResult(WriteResult.Result.OLD_VERSION);
                        	builder.setCurrentVersion(cur_version);
			}
		} else {
			builder.setResult(WriteResult.Result.NOT_LEADER);
		}
		WriteResult response = builder.build();
		responseObserver.onNext(response);
                responseObserver.onCompleted();
	}

	@Override
    	public void isLeader(surfstore.SurfStoreBasic.Empty request,
        	io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.SimpleAnswer> responseObserver) {
    		logger.info("Checking leader");
                SimpleAnswer response = SimpleAnswer.newBuilder().setAnswer(leader).build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();

	}
	
	@Override
	public void crash(surfstore.SurfStoreBasic.Empty request,
        	io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.Empty> responseObserver) {
    		logger.info("Crashed");
		crashed = true;
		Empty response = Empty.newBuilder().build();
            	responseObserver.onNext(response);
            	responseObserver.onCompleted();
	}

	@Override
    	public void restore(surfstore.SurfStoreBasic.Empty request,
        	io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.Empty> responseObserver) {
    		
	}

	@Override
    	public void isCrashed(surfstore.SurfStoreBasic.Empty request,
        	io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.SimpleAnswer> responseObserver) {
    		SimpleAnswer response = SimpleAnswer.newBuilder().setAnswer(crashed).build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
	}

	@Override
    	public void getVersion(surfstore.SurfStoreBasic.FileInfo request,
        	io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.FileInfo> responseObserver) {
    		logger.info("Getting version  " + request.getFilename());

                int version;

                if (metadataMap.containsKey(request.getFilename())) {
                        version = versionMap.get(request.getFilename());
                } else {
                        version = 0;
                }

                FileInfo.Builder builder = FileInfo.newBuilder();
                builder.setVersion(version);
                FileInfo response = builder.build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
	}

    }
}
