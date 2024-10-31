FROM golang:1.22-alpine AS buider

WORKDIR /af_parser

COPY go.mod go.sum ./
RUN go mod download
COPY . . 

EXPOSE 1111

CMD ["go", "run", "cmd/main/main.go"]