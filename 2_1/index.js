const { createReadStream } = require('fs')
const split = require('split2')
const { Client } = require('@elastic/elasticsearch');

(async function(){

  const client = new Client({
    node: 'http://localhost:9200',
    maxRetries: 5,
    requestTimeout: 60000,
    // sniffOnStart: true
  })

  // _cat/indices
  console.log((await client.cat.indices()).body)

  // delete index my-*
  let result_delete = await client.indices.delete({
    index: "my-*",
    allow_no_indices: true,
    ignore_unavailable: true
  });
  // console.log(result_delete);
  
  // _cat/indices
  console.log((await client.cat.indices()).body)

  // create index template
  await client.indices.putIndexTemplate({
    name: "my-template",
    cause: "for lesson",
    body: {
      "index_patterns": ["my-*"],
      "template": {
        "mappings": {
          "_source": {
            "enabled": true
          },
          "properties": {
            "user": {
              "type": "keyword"
            },
            "age": {
              "type": "long"
            },
            "@timestamp": {
              "type":   "date",
              "format": "epoch_second"
            }
          }
        },
      }
    }
  })


  const s = createReadStream('./dataset.ndjson')

  const b = client.helpers.bulk({
    datasource: s.pipe(split(JSON.parse)),
    // flushBytes: 10,
    onDocument (doc) {
      doc["@timestamp"] = Math.floor(Date.now() / 1000);
      return [
        { update: { _index: 'my-index', _id: doc["id"] } },
        { doc_as_upsert: true }
      ]
    },
    onDrop (doc) {
      console.log(doc);
      b.abort()
    }
  });

  s.on('end', ()=>{
    console.log('end!');
    console.log(b.stats);
  });


  console.log(b);

})();
