# bin/zookeeper-server-start.sh config/zookeeper.properties
# bin/kafka-server-start.sh config/server.properties

# Import necessary libraries
import asyncio
import json
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import Producer, Consumer, KafkaError
from bs4 import BeautifulSoup
import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import pandas as pd
from selenium.webdriver.support import expected_conditions
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import WebDriverException
from selenium.webdriver.common.by import By
import time 
from selenium.webdriver.safari.options import Options as SafariOptions


BROKER_URL = "localhost:9092"


def topic_exists(client, topic_name):
    client = AdminClient({"bootstrap.servers": BROKER_URL})
    topic_metadata = client.list_topics()
    if topic_metadata.topics.get(topic_name) is None:
        print("Topic doesnot exist")
        return False


def create_topic(client, topic_name):
    futures = client.create_topics(
        [
            NewTopic(topic_name,
                    num_partitions=1,
                    replication_factor=1,
                    config={"cleanup.policy":"delete","compression.type":"lz4","delete.retention.ms":"2","file.delete.delay.ms":"2"})
        ]
    )

    for _, future in futures.items():
        try:
            future.result()
            print("topic created")
        except Exception as e:
            print(f"failed to create topic {topic_name}: {e}")
            raise

async def produce_consume(topic_name):
    """Runs the Producer and Consumer tasks"""
    t1 = asyncio.create_task(produce(topic_name))
    t2 = asyncio.create_task(consume(topic_name))
    await t1
    await t2


async def produce(topic_name):
    """Produces data into the Kafka Topic"""
    p = Producer({"bootstrap.servers": BROKER_URL})

    # url = 'https://twitter.com/DogecoinInfo'
    url = "https://twitter.com/dogecoinrise"
    options = SafariOptions()
    driver = webdriver.Safari(options=options)
    driver.get(url)

    # Wait for tweets to populate the page
    try:
        WebDriverWait(driver, 60).until(expected_conditions.presence_of_element_located(
            (By.CSS_SELECTOR, '[data-testid="tweet"]')))
    except WebDriverException:
        print("Something happened. No tweets loaded")


    post_content = []
    for i in range(5):
        # scroll a bit for more tweets to appear
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(6)

    # Extract the HTML content of the page using BeautifulSoup
        html_content = driver.page_source
        soup = BeautifulSoup(html_content, 'html.parser')
        posts = soup.find_all(attrs={"data-testid": "tweetText"})
    # Extract the text content of each post
        new_posts = [post.text for post in posts]
    # Append the new posts to the existing ones
        post_content.extend(new_posts)

    # Save the scraped data in a CSV file
    data = pd.DataFrame({'post_content': post_content})
    data.to_csv('twitter_posts11.csv', index=False, mode='a', header=False)
    for tweet in post_content:
        curr_iteration = 0
        p.produce(topic_name, json.dumps({'tweet': tweet}).encode('utf-8'))
        print('Produced message: {0}'.format(tweet))
        curr_iteration += 1
        await asyncio.sleep(0.8)

    p.flush()    
    driver.quit()
    return post_content
   


async def consume(topic_name):
    """Consumes data from the Kafka Topic"""
    c = Consumer({"bootstrap.servers": BROKER_URL, "group.id": "0"})
    c.subscribe([topic_name])
    while True:
        message = c.poll(1.0)
        if message is None:
            print("no message received by consumer")
        elif message.error() is not None:
            print(f"error from consumer {message.error()}")
        else:
            print(f"consumed message {message.key()}: {message.value()}")
        await asyncio.sleep(2.5)




def main():
    """Checks for topic and creates the topic if it does not exist"""
    client = AdminClient({"bootstrap.servers": BROKER_URL})

    topic_name = "doge_tweets"
    
    exists = topic_exists(client, topic_name)
    print(f"Topic {topic_name} exists: {exists}")

    if exists is False:
        create_topic(client, topic_name)

    try:
        asyncio.run(produce_consume(topic_name))
    except KeyboardInterrupt as e:
        print("shutting down")        


if __name__ == "__main__":
    main()