# Use an official Golang runtime as the base image
FROM golang:1.21-alpine

# Set the working directory inside the container
WORKDIR /app

# Copy the Go source files to the container
COPY . .

# Build the Go server
RUN go mod tidy && go build -o rabbitMqToMongoDbLogger main.go

ENV RABBITMQ_HOST=localhost
ENV MONGODB_HOST=localhost

# Run the server (can override port with env variable)
CMD ["./rabbitMqToMongoDbLogger"]