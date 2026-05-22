package main

import (
	"log"
	"net"
	"os"
	"strings"

	pb "github.com/salaboy/spring-ai-agentic-webinar/shipping/gen/shippingpb"
	"github.com/salaboy/spring-ai-agentic-webinar/shipping/server"
	"github.com/segmentio/kafka-go"
	"google.golang.org/grpc"
)

func main() {
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		brokers = "localhost:9092"
	}
	topic := os.Getenv("KAFKA_TOPIC")
	if topic == "" {
		topic = "shipments"
	}

	kafkaWriter := &kafka.Writer{
		Addr:     kafka.TCP(strings.Split(brokers, ",")...),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}
	defer func() {
		if err := kafkaWriter.Close(); err != nil {
			log.Printf("Failed to close Kafka writer: %v", err)
		}
	}()

	lis, err := net.Listen("tcp", ":9091")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterShippingServiceServer(grpcServer, server.NewShippingServer(kafkaWriter))

	log.Printf("Shipping gRPC server listening on %s (kafka brokers=%s topic=%s)", lis.Addr(), brokers, topic)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
