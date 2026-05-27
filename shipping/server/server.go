package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	pb "github.com/salaboy/spring-ai-agentic-webinar/shipping/gen/shippingpb"
	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	tracerName = "shipping"
)

type ShipmentStatusEvent struct {
	ShipmentID string `json:"shipmentId"`
	Status     string `json:"status"`
	StatusDate string `json:"statusDate"`
}

type Shipment struct {
	ShipmentID            string
	OrderID               string
	ShippingAddress       *pb.Address
	ShipmentDate          time.Time
	EstimatedDeliveryDate time.Time
	Status                pb.ShipmentStatus
}

type ShippingServer struct {
	pb.UnimplementedShippingServiceServer

	mu        sync.Mutex
	shipments map[string]*Shipment
	counter   int
	kafka     *kafka.Writer
}

func NewShippingServer(kafkaWriter *kafka.Writer) *ShippingServer {
	return &ShippingServer{
		shipments: make(map[string]*Shipment),
		kafka:     kafkaWriter,
	}
}

func (s *ShippingServer) ShipOrder(ctx context.Context, req *pb.ShipOrderRequest) (*pb.ShipOrderResponse, error) {
	tracer := otel.Tracer(tracerName)
	ctx, span := tracer.Start(ctx, "ShipOrder",
		trace.WithAttributes(
			attribute.String("order.id", req.GetOrderId()),
		),
	)
	defer span.End()

	s.mu.Lock()
	defer s.mu.Unlock()

	s.counter++
	shipmentID := fmt.Sprintf("SHIP-%06d", s.counter)

	now := time.Now().UTC()
	estimatedDelivery := now.AddDate(0, 0, 5)

	shipment := &Shipment{
		ShipmentID:            shipmentID,
		OrderID:               req.GetOrderId(),
		ShippingAddress:       req.GetShippingAddress(),
		ShipmentDate:          now,
		EstimatedDeliveryDate: estimatedDelivery,
		Status:                pb.ShipmentStatus_PENDING,
	}
	s.shipments[shipmentID] = shipment

	span.SetAttributes(attribute.String("shipment.id", shipmentID))
	log.Printf("Shipment %s created for order %s", shipmentID, req.GetOrderId())

	// Publish "pending" event immediately
	s.publishStatusEvent(ctx, shipmentID, "pending")

	// Capture span context to link async status update spans back to this request.
	spanCtx := span.SpanContext()

	// Schedule async "shipped" after 3s and "delivered" after 10s
	go func() {
		time.Sleep(3 * time.Second)
		s.updateStatus(shipmentID, pb.ShipmentStatus_SHIPPED)
		asyncCtx, asyncSpan := tracer.Start(context.Background(), "ShipmentStatusUpdate",
			trace.WithLinks(trace.Link{SpanContext: spanCtx}),
			trace.WithAttributes(
				attribute.String("shipment.id", shipmentID),
				attribute.String("shipment.status", "shipped"),
			),
		)
		s.publishStatusEvent(asyncCtx, shipmentID, "shipped")
		asyncSpan.End()

		time.Sleep(10 * time.Second)
		s.updateStatus(shipmentID, pb.ShipmentStatus_DELIVERED)
		asyncCtx2, asyncSpan2 := tracer.Start(context.Background(), "ShipmentStatusUpdate",
			trace.WithLinks(trace.Link{SpanContext: spanCtx}),
			trace.WithAttributes(
				attribute.String("shipment.id", shipmentID),
				attribute.String("shipment.status", "delivered"),
			),
		)
		s.publishStatusEvent(asyncCtx2, shipmentID, "delivered")
		asyncSpan2.End()
	}()

	return &pb.ShipOrderResponse{
		ShipmentId:            shipmentID,
		ShipmentDate:          timestamppb.New(now),
		EstimatedDeliveryDate: timestamppb.New(estimatedDelivery),
		Status:                pb.ShipmentStatus_PENDING,
	}, nil
}

func (s *ShippingServer) ShippingDelay(ctx context.Context, req *pb.ShippingDelayRequest) (*pb.ShippingDelayResponse, error) {
	tracer := otel.Tracer(tracerName)
	_, span := tracer.Start(ctx, "ShippingDelay")
	defer span.End()

	const delay = 10 * time.Second
	log.Printf("ShippingDelay: sleeping for %s", delay)

	select {
	case <-time.After(delay):
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}

	return &pb.ShippingDelayResponse{
		Message:      "shipping delayed",
		DelaySeconds: int32(delay / time.Second),
	}, nil
}

func (s *ShippingServer) ShippingFailure(ctx context.Context, req *pb.ShippingFailureRequest) (*pb.ShippingFailureResponse, error) {
	tracer := otel.Tracer(tracerName)
	_, span := tracer.Start(ctx, "ShippingFailure")
	defer span.End()

	err := status.Error(codes.Internal, "shipping service failure")
	span.RecordError(err)
	log.Printf("ShippingFailure: returning %v", err)
	return nil, err
}

func (s *ShippingServer) updateStatus(shipmentID string, status pb.ShipmentStatus) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if shipment, ok := s.shipments[shipmentID]; ok {
		shipment.Status = status
	}
}

func (s *ShippingServer) publishStatusEvent(ctx context.Context, shipmentID, status string) {
	event := ShipmentStatusEvent{
		ShipmentID: shipmentID,
		Status:     status,
		StatusDate: time.Now().UTC().Format(time.RFC3339),
	}
	payload, err := json.Marshal(event)
	if err != nil {
		log.Printf("Failed to marshal %s event for shipment %s: %v", status, shipmentID, err)
		return
	}
	if err := s.kafka.WriteMessages(ctx, kafka.Message{
		Key:   []byte(shipmentID),
		Value: payload,
	}); err != nil {
		log.Printf("Failed to publish %s event for shipment %s: %v", status, shipmentID, err)
		return
	}
	log.Printf("Published %s event for shipment %s", status, shipmentID)
}
