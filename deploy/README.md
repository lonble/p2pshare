# Demo swarm (Docker Compose)

One command brings up 4 already-connected nodes - no manual `bootstrap` calls.

## Start

```sh
docker compose up --build
```

Wait for `bootstrap done.` in the logs of node2/node3/node4 (a few seconds).

Check from the host that everyone found each other:

```sh
./p2pc -api 127.0.0.1:8001 peers   # node1
./p2pc -api 127.0.0.1:8002 peers   # node2
./p2pc -api 127.0.0.1:8003 peers   # node3
./p2pc -api 127.0.0.1:8004 peers   # node4
```

| Node  | RPC (host)             | QUIC (host)  |
| ----- | ----------------------- | ------------ |
| node1 | http://127.0.0.1:8001/ | 127.0.0.1:9001 |
| node2 | http://127.0.0.1:8002/ | 127.0.0.1:9002 |
| node3 | http://127.0.0.1:8003/ | 127.0.0.1:9003 |
| node4 | http://127.0.0.1:8004/ | 127.0.0.1:9004 |

## Presentation script: redundancy demo

1. Publish a file on node1:
   ```sh
   ./p2pc -api 127.0.0.1:8001 publish /path/to/testfile.zip
   ```
   Copy the printed ID.

2. Download it on node2 (goes over the network, node2 becomes a provider too):
   ```sh
   ./p2pc -api 127.0.0.1:8002 download <ID> /tmp/out2
   ```

3. Kill node1 (the original publisher) while it's the only other copy:
   ```sh
   docker compose kill node1
   ```

4. Download on node3 - should still work, because node2 is now also a provider:
   ```sh
   ./p2pc -api 127.0.0.1:8003 download <ID> /tmp/out3
   ```

5. Bring node1 back and show its data survived (manifests persist to the
   named volume):
   ```sh
   docker compose start node1
   ./p2pc -api 127.0.0.1:8001 listFiles
   ```

## Frontend

Point the frontend at whichever node you want to demo with:

```
VITE_RPC_URL=http://127.0.0.1:8001/
```

You can even open the frontend twice (different `.env`/browser profiles)
pointed at two different nodes, to show publish-on-one/download-on-another
side by side.

## Adding more participants

Copy a `nodeN` block in `docker-compose.yml`, bump the host ports
(`800N:8000`, `900N:9000/udp`) and the volume name, keep
`BOOTSTRAP_HOST: node1`, add the new volume under `volumes:`.

## Cleanup

```sh
docker compose down          # stop, keep data volumes
docker compose down -v       # stop and wipe all node data
```
