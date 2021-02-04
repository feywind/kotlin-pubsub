import com.google.api.gax.core.CredentialsProvider
import com.google.cloud.pubsub.v1.*
import com.google.protobuf.ByteString
import com.google.pubsub.v1.ProjectSubscriptionName
import com.google.pubsub.v1.PubsubMessage
import com.google.pubsub.v1.PushConfig
import com.google.pubsub.v1.TopicName
import java.util.*
import kotlin.concurrent.schedule
import com.google.api.gax.core.NoCredentialsProvider
import com.google.api.gax.grpc.GrpcTransportChannel
import com.google.api.gax.rpc.FixedTransportChannelProvider
import com.google.api.gax.rpc.NotFoundException
import io.grpc.ManagedChannelBuilder
import io.grpc.ManagedChannel

// Options for the sample.
const val projectId = "local-dev"
const val apiEndpoint = "localhost:8085"
const val topicId = "test-topic"
const val subId = "test-sub"

// Create Java library names for use below.
val topicName: TopicName = TopicName.of(projectId, topicId)
val subName: ProjectSubscriptionName = ProjectSubscriptionName.of(projectId, subId)

// This is needed to bypass SSL for the emulator. In your actual
// application, you would just leave this out of the settings when
// creating the clients below.
val channel: ManagedChannel = ManagedChannelBuilder.forTarget(apiEndpoint)
    .usePlaintext()
    .build()
val channelProvider: FixedTransportChannelProvider = FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel))
val credentialsProvider: CredentialsProvider = NoCredentialsProvider.create()

// Gets a publisher for our test topic, creating it if needed.
fun getPublisher(): Publisher {
    // Create the topic if it doesn't exist.
    val settings = TopicAdminSettings.newBuilder()
        .setTransportChannelProvider(channelProvider)
        .setCredentialsProvider(credentialsProvider)
        .build()
    TopicAdminClient.create(settings).use { topicAdminClient ->
        try {
            topicAdminClient.getTopic(topicName)
        } catch (e: NotFoundException) {
            topicAdminClient.createTopic(topicName)
        }
    }

    return Publisher.newBuilder(topicName)
        .setChannelProvider(channelProvider)
        .setCredentialsProvider(credentialsProvider)
        .setEndpoint(apiEndpoint)
        .build()
}

// Verifies that the subscription exists, and makes it if not.
fun ensureSubscription() {
    // Create the subscriber if it doesn't exist.
    val settings = SubscriptionAdminSettings.newBuilder()
        .setTransportChannelProvider(channelProvider)
        .setCredentialsProvider(credentialsProvider)
        .build()
    SubscriptionAdminClient.create(settings).use { subAdminClient ->
        try {
            subAdminClient.getSubscription(subName)
        } catch (e: NotFoundException) {
            subAdminClient.createSubscription(
                subName, topicName,
                PushConfig.getDefaultInstance(), 0
            )
        }
    }
}

// Builds a subscriber and waits for the user to end the wait for messages.
fun runSubscriber(receiver: MessageReceiver) {
    ensureSubscription()

    val sub = Subscriber.newBuilder(subName, receiver)
        .setChannelProvider(channelProvider)
        .setCredentialsProvider(credentialsProvider)
        .setEndpoint(apiEndpoint)
        .build()

    try {
        sub.startAsync().awaitRunning()
        println("Listening for messages on $subName. Press ENTER to quit.")
        readLine()
    } catch (e: Exception) {
        println("Stopped due to exception: ${e.message}, ${e.stackTraceToString()}")
        sub.stopAsync()
    }
}

fun main() {
    // Set up a timer that will show us how many messages are being received.
    var messageCount = 0
    val messageLock = Object()
    Timer("schedule", true).schedule(delay = 0, period = 10*1000) {
        synchronized(messageLock) {
            println("Received $messageCount messages")
            messageCount = 0
        }
    }

    // Publish 500 messages.
    val message = "Hello World!"
    val data = ByteString.copyFromUtf8(message)
    val pubsubMessage = PubsubMessage.newBuilder()
        .setData(data)
        .build()

    val publisher = getPublisher()
    for (i in 1..500) {
        publisher.publish(pubsubMessage)
    }

    // Run the subscriber to catch and ack all the messages.
    runSubscriber { _, consumer ->
        synchronized(messageLock) {
            messageCount++
        }
        consumer.ack()
    }
}
