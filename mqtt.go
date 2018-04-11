package mqtt

import (
	"github.com/eclipse/paho.mqtt.golang"
)


type Mqtt struct {
	client mqtt.Client
}

type Message struct {
	Topic string
	Payload []byte
}

func New(brokerAddress string, clientName string) (mqttClient Mqtt, err error) {
	client := createMqttClient(brokerAddress, clientName)
	err = connectToClient(client)
	mqttClient =  Mqtt{client:client}
	return
}

func createMqttClient(brokerAddress string, clientName string) mqtt.Client{
	var mqttClientOptions = mqtt.NewClientOptions().AddBroker(brokerAddress).SetClientID(clientName);
	return mqtt.NewClient(mqttClientOptions)
}

func connectToClient(client mqtt.Client) error {
	if token := client.Connect(); token.Wait() && token.Error() != nil{
		return token.Error()
	}
	return nil
}

func (mqttClient Mqtt) SubscribeToTopic(topic string, callback func(msg Message)) error {
	token := mqttClient.client.Subscribe(topic, 0, func(client mqtt.Client, message mqtt.Message) {
		callback(Message{message.Topic(), message.Payload()})
	});

	if token.Wait() && token.Error() != nil {
		return token.Error()
	}
	return nil
}

func (mqttClient Mqtt) Disconnect(quiesce uint) {
	mqttClient.client.Disconnect(quiesce)
}

func (mqttClient Mqtt) PublishToTopic(topic string, data []byte) error{
	token := mqttClient.client.Publish(topic, 0, false, data);

	if token.Wait() && token.Error() != nil {
		return token.Error()
	}
	return nil
}

func (mqttClient Mqtt) PublishStringToTopic(topic string, data string) error{
	token := mqttClient.client.Publish(topic, 0, false, data);

	if token.Wait() && token.Error() != nil {
		return token.Error()
	}
	return nil
}