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
