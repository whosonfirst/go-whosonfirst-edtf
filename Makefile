cli:
	go build -mod vendor -o bin/find-invalid cmd/find-invalid/main.go
	go build -mod vendor -o bin/update-unknown-uncertain cmd/update-unknown-uncertain/main.go
