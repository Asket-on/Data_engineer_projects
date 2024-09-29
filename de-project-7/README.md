# The 7th Project

### Description
The food delivery aggregator is gaining popularity and is introducing a new option - subscription. It opens up a number of possibilities for users, one of which is to add restaurants to favorites. Only then will the user receive notifications about special promotions with a limited period of validity. A system that will help implement this update will need to be created here.

The system works like this:
1. The restaurant sends a limited-time promotion through its mobile app. For example, something like this: “Here’s a new dish - it’s not on the regular menu. We are giving a 70% discount on it until 14:00! Every comment about the new product is important to us.”
2. The service checks which user has a restaurant in their favorites list.
3. The service generates templates for push notifications to these users about temporary promotions. Notifications will only be sent while the promotion is valid.

The task is to pick up messages from Kafka, process them and send them to two receivers: a Postgres database and a new topic for the kafka broker.
