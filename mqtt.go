package mqtt

import (
	"github.com/eclipse/paho.mqtt.golang"
	"errors"
	"time"
)


type Client struct {
	client mqtt.Client
}

type Message struct {
	Topic string
	Payload []byte
}

func New(brokerAddress string, clientName string) (mqttClient Client, err error) {
	client := createMqttClient(brokerAddress, clientName)
	err = connectToClient(client)
	mqttClient =  Client{client:client}
	return
}

func createMqttClient(brokerAddress string, clientName string) mqtt.Client{
	var mqttClientOptions = mqtt.NewClientOptions().AddBroker(brokerAddress).SetClientID(clientName);
	return mqtt.NewClient(mqttClientOptions)
}

func connectToClient(client mqtt.Client) error {
	var err = errors.New("")

	for i:=0; i<10; i++ {
		token := client.Connect()
		if token.Wait() && token.Error() != nil{
			err = token.Error()
			time.Sleep(time.Second)
			continue
		}
		return nil
	}
	return err
}

func (mqttClient Client) SubscribeToTopic(topic string, callback func(msg Message)) error {
	token := mqttClient.client.Subscribe(topic, 0, func(client mqtt.Client, message mqtt.Message) {
		callback(Message{message.Topic(), message.Payload()})
	});

	if token.Wait() && token.Error() != nil {
		return token.Error()
	}
	return nil
}

func (mqttClient Client) Disconnect(quiesce uint) {
	mqttClient.client.Disconnect(quiesce)
}

func (mqttClient Client) PublishToTopic(topic string, data []byte) error{
	token := mqttClient.client.Publish(topic, 0, false, data);

	if token.Wait() && token.Error() != nil {
		return token.Error()
	}
	return nil
}

func (mqttClient Client) PublishStringToTopic(topic string, data string) error{
	token := mqttClient.client.Publish(topic, 0, false, data);

	if token.Wait() && token.Error() != nil {
		return token.Error()
	}
	return nil
}