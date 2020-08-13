const puppeteer = require('puppeteer')
const axios = require('axios')
const redis = require('redis');
const fs = require('fs');
const scrape = require('website-scraper');
const PuppeteerPlugin = require('website-scraper-puppeteer');
var path = require('path');

const client = redis.createClient();
// Print redis errors to the console
client.on('error', (err) => {
  console.log("Error " + err);
}).on("connect", function (error) {
  console.log("Redis is connected");
})

const kafkaWork = (producer, job, keyword, data, echo=null) => {
    payloads = [
      { 
        topic: 'test4', 
        key: job.data.store, 
        messages: JSON.stringify({ store: job.data.store, keyword: keyword, data: data, echo: echo}) }
      // {topic: "progress", key: job.data.store, messages: JSON.stringify({store: job.data.store, keyword:keyword, data: progress})},
    ]
    producer.send(payloads, function (err, data) {
      console.log('streaming data', { store: job.data.store, keyword: keyword })
    })
    producer.on('error', function (err) {
      console.log(err)
    })
  }
  
  const handlerDispatcher = (store, keyword) => {
    console.log(keyword)
    if (keyword.includes("product%3A")) {
      return handlers_product[store]
    }
    return handlers[store]
  }
  
  const handleJobs = async (store, keywords, isUpdate, job = null, producer = null, echo = null) => {
    return new Promise(function (resolve, reject) {
      const res = []
      let items = 0
      keywords.forEach((keyword) => {
        items += 1
        keyword = encodeURIComponent(keyword)
        client.get(`${store}:${keyword}`, (err, data) => {
          //if update is request or there is no data
          // if (isUpdate || !data) {
          if (true) {
            console.log("retrieving from web")
            handlerDispatcher(store, keyword)(keyword).then(
              data => {
                res.push({ [keyword]: data})
                if (job) kafkaWork(producer, job, keyword, data, JSON.parse(echo))
                client.set(`${store}:${keyword}`, JSON.stringify(data), function (err) {
                  if (err != null) console.error(err);
                })
                progress = (items / keywords.length) * 100
                if (items == keywords.length) resolve(res)
              }
            ).catch(err => {
              reject(err)
            })
          }
          else {
            console.log("retrieving from cache")
            res.push({ [keyword]: JSON.parse(data) })
            if (job) kafkaWork(producer, job, keyword, data)
            progress = (items / keywords.length) * 100
            if (items == keywords.length) resolve(res)
          }
        })
      })
    })
  }
  
  const prod = {
    p_name: null,
    url: null,
    uuid: null,
    price: null,
    regularPrice: null,
    img: null
  }
  