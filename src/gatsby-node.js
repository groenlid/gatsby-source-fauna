const { query, Client, values } = require('faunadb')
const { parseJSON, toJSON } = require('faunadb/src/_json')
const q = query;

function sanitizeName(s) {
  return s.replace(/[^_a-zA-Z0-9]/g, ``).replace(/\b\w/g, l => l.toUpperCase())
}

function faunaLogger (response) {
  console.log(`Faunadb query used ${response['responseHeaders']['x-read-ops']} read-ops that took ${response.endTime - response.startTime} ms`)
}

exports.sourceNodes = async (
  sourceNodesOpt,
  pluginOptions
) => {

  const { secret, collections } = pluginOptions

  const client = new Client({ secret, observer: faunaLogger})

  try {

    await Promise.all(collections.map(collection => createNodes(sourceNodesOpt, client, collection)))

  } catch(err) {
    console.error(err)
  }

}

function createNodeIdImp(createNodeId, collectionName, id) {
  return createNodeId(`faunadb-${sanitizeName(collectionName)}-${id}`)
}

async function fetchAllData(client, collection) {
  console.log(`No cache found from previous build. Fetching all data`)
  const data = []

  let after = null

  do {
    const paginatedOpt = after ? {
      after
    } : undefined
    const result = await client.query(q.Map(q.Paginate(q.Documents(q.Collection(collection.name)), paginatedOpt), q.Lambda(x => q.Get(x))))
    data.push(...result.data)
    after = result.after
  } while (!!after)

  const timestamp = Math.max(...data.map(d => d.ts))
  return [timestamp, data]
}

function getCacheKey(collection) {
  return `faunadb-${collection.name}`;
}

async function fetchDiffData(client, collection, {
  timestamp,
  data
}) {
  let newData = data;
  let newTimestamp = timestamp
  console.log(`Cache found from previous build. Fetching data since: ${timestamp} for collection ${collection.name}`)
  console.time(`fetching delta for ${collection.name}`)

  // Fetching the updated and created documents in the collection
  let after = timestamp + 1;

  // Finding the created and updated documents
  const createdAndUpdated = await client.query(
    q.Map(
      q.Paginate(
        q.Range(q.Match(q.Index(collection.tsIndex)), after, null),
      ),
      q.Lambda(['ts', 'ref'], q.Paginate(q.Var('ref'), { events: true, after }))
    )
  )
  console.log(createdAndUpdated)
  

  // Fetching the updated and created documents
  const updatedAndCreatedDocuments = await client.paginate(
    q.Range(q.Match(q.Index(collection.tsIndex)), after, null)
  )

  await updatedAndCreatedDocuments.each((page) => {
    for (const document of page) {
      console.log(document)
      newTimestamp = document.ts > newTimestamp ? document.ts : newTimestamp;
    }
  })

  // Fetching the deleted documents
  const deletedAndCreatedDocuments = await client.paginate(
    q.Documents(q.Collection(collection.name)),
    { after, events: true }
  )

  await deletedAndCreatedDocuments.each((page) => {
    for (const event of page) {
      console.log(event)
      newTimestamp = event.ts > newTimestamp ? event.ts : newTimestamp;
      if(event.action !== 'remove') continue
      newData = newData.filter(document => !document.ref.equals(event.instance))
    }
  })

  console.log(`Number of datapoints ${data.length}`)

  console.timeEnd(`fetching delta for ${collection.name}`)

  // We are still returning the old data and timestamp because the delta fetching is not working yet.
  return [timestamp, data] //return [newTimestamp, data];
}

async function fetchData(client, collection, cache) {
  const cacheKey = getCacheKey(collection);
  const value = await cache.get(cacheKey);
  const [timestamp, data] = value ? await fetchDiffData(client, collection, parseJSON(value)) : await fetchAllData(client, collection);
  
  await cache.set(cacheKey, toJSON({
    timestamp,
    data
  }));
  return data;
}

async function createNodes({
  actions,
  getNode,
  createNodeId,
  hasNodeChanged,
  createContentDigest,
  cache
}, client, collection) {
  const {
    createNode
  } = actions;
  const data = await fetchData(client, collection, cache);

  for (const document of data) {
    const id = document.ref.id || document.ref["@ref"].id;

    if (document.data == null) {
      return;
    }

    const node = { ...document.data,
      id: createNodeIdImp(createNodeId, collection.name, id),
      _id: id,
      _ts: document.ts,
      parent: null,
      children: [],
      internal: {
        type: `faunadb${sanitizeName(collection.name)}`,
        content: JSON.stringify(document.data),
        contentDigest: createContentDigest(document.data)
      }
    };

    for (const [key, value] of Object.entries(document.data)) {
      if (value instanceof values.Ref) {
        node[`${key}___NODE`] = createNodeIdImp(createNodeId, value.collection.value.id, value.value.id);
      } //console.log('Found a ref!!!')

    }

    createNode(node);
  }
}