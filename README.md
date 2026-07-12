# p2pshare

## Go Environment Setup

For **Debian 13**, use the following command to install the Go toolchain, The corresponding Go version is **1.24**.

```sh
apt install golang-go
```

Modern programming languages like Go tend to clutter the user's home directory. Furthermore, Go's default behavior will absurdly auto-upgrade the toolchain based on dependency requirements. We need some extra configurations to make it behave decently.

```sh
go env -w GOBIN=$HOME/.local/bin
go env -w GOPATH=$HOME/.cache/go
go env -w GOTOOLCHAIN=local
```

If you use **VS Code** as your IDE, **golang.go** extension is all you need.

## Build

Run the following command to build. It automatically resolves dependencies and downloads everything you need.

```sh
go build ./cmd/...
```

If needed, you can build and then move the generated binary to `$HOME/.local/bin` directory using the following command.

```sh
go install ./cmd/...
```

## Usage

```
Usage of p2pshare:
  -addr string
        QUIC listen address (default ":9000")
  -data string
        Data directory (default "./p2pdata")
  -rpc string
        HTTP JSON-RPC address (default "127.0.0.1:8000")
```

```
P2P Share CLI Client

Usage:
  p2pc [flags] <command> [arguments]

Commands:
  status                   Show node status
  peers                    List connected peers
  listFiles                List published files
  publish <path>           Publish a local file
  download <id> [outdir]   Download file (outdir defaults to '.')
  bootstrap <id,addr>...   Bootstrap DHT

Flags:
  -api string
        Server API Address (default "127.0.0.1:8000" or P2P_API env)
```

## Principle

### Peer Discovery

Without any centralized technology, node discovery must rely on manually specified bootstrap nodes. The software adds bootstrap nodes as peers, then queries for the closest nodes using the Kademlia algorithm to fill peer buckets.

Thereafter, any node connecting to the current node or targeted by an outbound connection will become a new peer, updating peer buckets under the LRU policy. During the bucket update process, priority is given to retaining older, active nodes.

### File Organization

A file is split into multiple chunks, each having a unique ID (generated via hashing). Information such as the IDs of all the chunks of the file, the filename, and the file size is stored in a JSON data structure called a manifest. The manifest itself is also stored as a chunk.

### File Publish

First, split the file into chunks and calculate each chunk's ID. Then, use the Kademlia algorithm to find the nodes closest to that ID, and request those nodes to register this node as the provider of the chunk represented by the ID.

After all chunks are processed in this manner, the manifest chunk is generated. The same operation is then performed on the manifest chunk. Finally, the ID of the manifest is returned.

Since provider records are only kept in memory and are subject to expiration, a node will periodically republish all chunks they hold.

### File Download

Files are downloaded using the manifest ID.

First, we use the Kademlia algorithm to iteratively search for the providers of the chunk represented by this ID, and then connect to a provider to download the chunk. 

Once the manifest is retrieved, we download each individual chunk based on the chunk IDs recorded in the manifest. Different chunks can be downloaded concurrently. If multiple providers exist for a single chunk, the provider list is randomized. Therefore, different parts of a file can be downloaded simultaneously from multiple different nodes, making full use of the network.

For each downloaded chunk (including manifest chunk), its ID is recalculated to ensure data integrity. The chunk is then stored locally, and the node registers itself as a provider of that chunk to the DHT network to facilitate file distribution. 

After all chunks are downloaded, the file is reassembled based on the metadata in the manifest and saved to the specified directory.

### Network

Inter-node interaction is based on QUIC, which brings state-of-the-art security. Meanwhile, by leveraging QUIC's connection multiplexing, the overhead of TLS handshakes can be drastically reduced during high-frequency connections.

The listening port of QUIC can be configured to any port without affecting the upper-layer logic. However, we use the same port for both listening and sending to simplify the protocol design and facilitate NAT and firewall traversal to a certain extent.

### Security

Security is the primary feature of our project. Our file IDs are generated using the cryptographically secure SHA-256 algorithm. Connections are established over QUIC, supporting the latest TLS 1.3 protocol and offering post-quantum security.

We achieve comprehensive security by pairing self-certification with QUIC. Conventional TLS is highly dependent on centralized infrastructure and demands rigid certificate enrollment processes to defend against Man-in-the-Middle attacks. However, it is highly impractical to deploy within P2P networks. Self-certification addresses this by generating node IDs from the nodes' own public keys. This allows endpoints to verify peer identities and securely complete key exchanges without the need for a centralized Certificate Authority.
