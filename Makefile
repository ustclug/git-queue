BIN := git-proxy

.PHONY: all $(BIN)

all: $(BIN)

$(BIN):
	go build -ldflags='-s -w'
