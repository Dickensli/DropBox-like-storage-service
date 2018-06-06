package surfstore;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.logging.Logger;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;

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

import java.util.ConcurrentModificationException;

public final class MetadataStore {
	private static final Logger logger = Logger.getLogger(MetadataStore.class.getName());

	private final ManagedChannel blockChannel;
	private static BlockStoreGrpc.BlockStoreBlockingStub blockStub;
	private final ArrayList<ManagedChannel> metadataChannelList;
	private static ArrayList<MetadataStoreGrpc.MetadataStoreBlockingStub> metadataStubList;
	private static ArrayList<HashMap<String, Integer>> followerMapList;
	private static ArrayList<surfstore.SurfStoreBasic.FileInfo> updatedFiles;

	protected Server server;
	private static ConfigReader config;
	private static boolean leader;
	private static boolean crashed;

	public MetadataStore(ConfigReader config, Namespace c_args) {
        	this.config = config;
        	this.leader = config.getLeaderNum() == c_args.getInt("number");
        	this.crashed = false;

		this.blockChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getBlockPort()).usePlaintext(true).build();
                this.blockStub = BlockStoreGrpc.newBlockingStub(blockChannel);
		this.metadataChannelList = new ArrayList<ManagedChannel>();
                this.metadataStubList = new ArrayList<MetadataStoreGrpc.MetadataStoreBlockingStub>();
                this.followerMapList = new ArrayList<HashMap<String, Integer>>();
		this.updatedFiles = new ArrayList<surfstore.SurfStoreBasic.FileInfo>();

        	// leader
		if (this.leader) {
                        for (int i = 1; i <= config.getNumMetadataServers(); i++){
				if (i != config.getLeaderNum()){
					ManagedChannel metadataChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getMetadataPort(i)).usePlaintext(true).build();
					MetadataStoreGrpc.MetadataStoreBlockingStub metadataStub = MetadataStoreGrpc.newBlockingStub(metadataChannel);
					this.metadataChannelList.add(metadataChannel);
					this.metadataStubList.add(metadataStub);
					HashMap<String, Integer> followerMap = new HashMap<String, Integer>();
					this.followerMapList.add(followerMap);
				}
			}
		}	
	}
	// update followers during heartbeat
	private int update(surfstore.SurfStoreBasic.FileInfo request, String method) {
                int updated = 0;
                for (int i = 0; i < metadataStubList.size(); i++) {
                        MetadataStoreGrpc.MetadataStoreBlockingStub metaStub = metadataStubList.get(i);
                        metaStub.ping(Empty.newBuilder().build());
                        // 1st phase
			if (metaStub.updateLog(LogInfo.newBuilder().setLog(method).setFilename(request.getFilename()).build()).getAnswer() == false) { // not crashed
				// 2nd phase
				if (metaStub.updateFollower(request).getResult() == WriteResult.Result.OK) {
					followerMapList.get(i).put(request.getFilename(), request.getVersion());
					updated++;
				}
			}
		}
		logger.info("heartbeat update follower numbers " + Integer.toString(updated));
		return updated;
	}
	
	private void heartbeat() {
		while (true) {
			try { // sleep for 500ms
				Thread.sleep(500);
			}
			catch (InterruptedException e) {System.err.println(e);}
			
			if (updatedFiles.size() > 0) {
				logger.info("before update: updatedFiles contains file numbers " + Integer.toString(updatedFiles.size()));
				ArrayList<surfstore.SurfStoreBasic.FileInfo> successFile = new ArrayList<surfstore.SurfStoreBasic.FileInfo>();
				ArrayList<surfstore.SurfStoreBasic.FileInfo> currentFile = new ArrayList<surfstore.SurfStoreBasic.FileInfo>(updatedFiles);
				for (Iterator<surfstore.SurfStoreBasic.FileInfo> iterator = currentFile.iterator(); iterator.hasNext(); ) {
					surfstore.SurfStoreBasic.FileInfo file = iterator.next();
					String method = "catch up";
					if (update(file, method) + 1 == config.getNumMetadataServers()) {
						successFile.add(file);
					}
				} 
				try {
					updatedFiles.removeAll(successFile);
				} catch (ConcurrentModificationException e) {
                                        logger.info("concurrent modifying updatedFiles");
				}

				logger.info("after update: updateFiles contains file numbers " + Integer.toString(updatedFiles.size()));
			}
		}
	}

	private void start(int port, int numThreads) throws IOException {
        server = ServerBuilder.forPort(port)
                .addService(new MetadataStoreImpl())
                .executor(Executors.newFixedThreadPool(numThreads))
                .build()
                .start();
        logger.info("Server started, listening on " + port);
        // leader heartbeat
	if (leader) {
		Thread heartbeatThread = 
		    new Thread(
			new Runnable() {
			    @Override
			    public void run() {
				heartbeat();	
			    }
			});
		heartbeatThread.setName("grpc-heartbeat");
		heartbeatThread.setDaemon(true);
		heartbeatThread.start();
	}
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
			logger.info("read file get version " + Integer.toString(version));
		} else {
			hashlist = new ArrayList<String>();
			version = 0; 
			logger.info("read file no such file");
		}
		iter_hashlist = hashlist;

		FileInfo.Builder builder = FileInfo.newBuilder();
		builder.setFilename(request.getFilename());
		builder.setVersion(version);
		builder.addAllBlocklist(iter_hashlist); 
		FileInfo response = builder.build();
		
		responseObserver.onNext(response);
		responseObserver.onCompleted();
	}
	
	public void updateLog(surfstore.SurfStoreBasic.LogInfo request,
        	io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.SimpleAnswer> responseObserver) {
        	logger.info("Updating log in follower");
		
		SimpleAnswer.Builder builder = SimpleAnswer.newBuilder();

		if (!crashed) {
			logger.info("Log method " + request.getLog() + " on file " + request.getFilename());
		}
		SimpleAnswer response = builder.setAnswer(crashed).build();
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

	// 2nd phase 
        @Override
	public void updateFollower(surfstore.SurfStoreBasic.FileInfo request,
        io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.WriteResult> responseObserver) {
		
		WriteResult.Builder builder = WriteResult.newBuilder();

		if (!crashed){
			logger.info("Updating file " + request.getFilename());
			versionMap.put(request.getFilename(), request.getVersion());
			ArrayList<String> hashlist = new ArrayList<String>(request.getBlocklistList());
			metadataMap.put(request.getFilename(), hashlist);
			builder.setResult(WriteResult.Result.OK);
		} else {
			logger.info("Crashed during updating file " + request.getFilename());
			builder.setResult(WriteResult.Result.OLD_VERSION);
		}

		WriteResult response = builder.build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
	}

	private int twoPhase(surfstore.SurfStoreBasic.FileInfo request, String method) {
		int updated = 0;
		logger.info("two phase");
		for (int i = 0; i < metadataStubList.size(); i++) {
			MetadataStoreGrpc.MetadataStoreBlockingStub metaStub = metadataStubList.get(i);
			metaStub.ping(Empty.newBuilder().build());
			// 1st phase
			logger.info("leader successfully ping follower");
			if (metaStub.updateLog(LogInfo.newBuilder().setLog(method).setFilename(request.getFilename()).build()).getAnswer() == false) { // not crashed
				// 2nd phase
				logger.info("follower not crashed");
				if (metaStub.updateFollower(request).getResult() == WriteResult.Result.OK) {
					followerMapList.get(i).put(request.getFilename(), request.getVersion());
					updated++;
				}
			}
		}
		return updated;
	}
	
	@Override
    	public void modifyFile(surfstore.SurfStoreBasic.FileInfo request,
        	io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.WriteResult> responseObserver) {
		
		WriteResult.Builder builder = WriteResult.newBuilder();
		if (leader){ // is leader
			logger.info("Modifying file  " + request.getFilename());
			int cur_version = 0;
			if(versionMap.containsKey(request.getFilename())){
                		cur_version = versionMap.get(request.getFilename());
				logger.info("current version is " + Integer.toString(cur_version));
			}


                	ArrayList<String> hashlist = new ArrayList<String>(request.getBlocklistList()); 
			logger.info("request get version " + Integer.toString(request.getVersion()));
			logger.info("1");
               		ArrayList<String> misslist;
                	Iterable<String> iter_misslist;
			if (request.getVersion() == cur_version + 1) { /* valid */
				// check missing blocks
				misslist = findBlock(hashlist);
				logger.info("get into valid version");
				if (!misslist.isEmpty()){
					iter_misslist = misslist;
					for (String miss: misslist){
						logger.info(miss);
					}
					logger.info("2");
					builder.setResult(WriteResult.Result.MISSING_BLOCKS);
                			builder.setCurrentVersion(cur_version);
					builder.addAllMissingBlocks(iter_misslist); 
				} else { // no missing blocks
					// update followers - success if more than half of servers are updated
					logger.info("no missing blocks");
					String method = "modify";
					int twoPCsuccess = twoPhase(request, method);
					logger.info("twoPCsuccess is " + Integer.toString(twoPCsuccess));
					logger.info("number of servers is " + Integer.toString(config.getNumMetadataServers()));
					if (2 * (twoPCsuccess + 1) > config.getNumMetadataServers()){
						logger.info("can update maps");
						versionMap.put(request.getFilename(),  request.getVersion());
                                        	metadataMap.put(request.getFilename(), hashlist);
						if (twoPCsuccess + 1 < config.getNumMetadataServers()){
                                        		// not all followers updated
							updatedFiles.add(request);
						}

						logger.info("version map getversion is " + versionMap.get(request.getFilename()));
						builder.setResult(WriteResult.Result.OK);
                                        	builder.setCurrentVersion(request.getVersion());
					} else { // more than half of servers crashed
						builder.setResult(WriteResult.Result.OLD_VERSION);
						builder.setCurrentVersion(cur_version);
					}
				}
			} else { /* not valid */
				logger.info("not valid version");
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
    	public void deleteFile(surfstore.SurfStoreBasic.FileInfo request,
        	io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.WriteResult> responseObserver) {

		WriteResult.Builder builder = WriteResult.newBuilder();
		if (leader) {
			logger.info("Deleting file  " + request.getFilename());
			if (metadataMap.containsKey(request.getFilename()) && !metadataMap.get(request.getFilename()).get(0).equals("0")) {
				int cur_version = versionMap.get(request.getFilename());
				logger.info("can delete file " + request.getFilename());
				FileInfo new_request = FileInfo.newBuilder().setFilename(request.getFilename()).setVersion(cur_version + 1).build();
				String method = "delete";
				int twoPCsuccess = twoPhase(new_request, method);
				logger.info("2pc success num is " + Integer.toString(twoPCsuccess));
				if (2 * (twoPCsuccess + 1) > config.getNumMetadataServers()){ 
					// update maps
					logger.info("can update map");
					ArrayList<String> hashlist = new ArrayList<String>();
					String hash = "0";
					hashlist.add(hash);
					versionMap.put(request.getFilename(), cur_version + 1);
					metadataMap.put(request.getFilename(), hashlist);
					if (twoPCsuccess + 1 < config.getNumMetadataServers()){
						// not all followers updated
						updatedFiles.add(new_request);
					}
					builder.setResult(WriteResult.Result.OK);
					builder.setCurrentVersion(cur_version);
				} else { // more than half of servers crashed
					builder.setResult(WriteResult.Result.OLD_VERSION);
                                	builder.setCurrentVersion(cur_version);
				}
			} else { // not in maps
				logger.info("cannot delte file");
				builder.setResult(WriteResult.Result.OLD_VERSION);
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
    		logger.info("Restored");
		crashed = false;
		Empty response = Empty.newBuilder().build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();	
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
