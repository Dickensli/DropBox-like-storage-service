## Overview

this is a cloud-based file storage service called SurfStore. SurfStore is a networked file storage application. Multiple clients can concurrently connect to the SurfStore service to access a common, shared set of files. Clients accessing SurfStore “see” a consistent set of updates to files, but SurfStore does not offer any guarantees about operations across files, meaning that it does not support multi-file transactions (such as atomic move).

___The SurfStore service is composed of the following two sub-services___:

**BlockStore**: The content of each file in SurfStore is divided up into chunks, or blocks, each of which has a unique identifier. The BlockStore service stores these blocks, and when given an identifier, retrieves and returns the appropriate block.

**MetadataStore**: The MetadataStore service holds the mapping of filenames/paths to blocks. The MetadataStore service is a set of distributed processes that implement fault tolerance. This distributed implementation will use a repliated log (replicated state machine) plus 2-phase commit to ensure that the MetadataStore service can survive, and continue operating, even if one of its processes fails, and that after failed processes recover they are able to rejoin the distributed system and get up-to-date.
## Usage

## To build the protocol buffer IDL into auto-generated stubs:

$ mvn protobuf:compile protobuf:compile-custom

## To build the code:

$ mvn package

## To run the services:

$ target/surfstore/bin/runBlockServer
$ target/surfstore/bin/runMetadataStore

## To run the client

$ target/surfstore/bin/runClient
