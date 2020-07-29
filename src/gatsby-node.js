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
  const data = {}

  let after = null
  let timestamp = null
  do {
    const paginatedOpt = after ? {
      after
    } : undefined
    const result = await client.query(q.Map(q.Paginate(q.Documents(q.Collection(collection.name)), paginatedOpt), q.Lambda(x => q.Get(x))))
    for(const document of result.data) {
      data[getIdOfDocumentOrEvent(document)] = document
      timestamp = Math.max(document.ts, timestamp)
    }
    after = result.after
  } while (!!after)
  return [timestamp, data]
}

function getCacheKey(collection) {
  return `faunadb-${collection.name}`;
}

async function fetchDeletedRefsSinceTimestamp(client, collection, timestamp) {
  console.time(`fetching deleted documents for ${collection.name} since ${timestamp}`)
  let after = timestamp + 1
  let latestTimestampOfEvent = undefined
  let deletedIds = []

  const deletedAndCreatedDocuments = await client.paginate(
    q.Documents(q.Collection(collection.name)),
    { after, events: true }
  )

  await deletedAndCreatedDocuments.each((page) => {
    for (const event of page) {
      if(event.action !== 'remove') continue
      latestTimestampOfEvent = event.ts > latestTimestampOfEvent ? event.ts : latestTimestampOfEvent;
      deletedIds.push(getIdOfDocumentOrEvent(event))
    }
  })

  return [latestTimestampOfEvent, deletedIds]
}

async function fetchCreatedAndUpdatedDocumentsSinceTimestamp(client, collection, timestamp) {
  let after = timestamp + 1
  let latestTimestampOfEvent = undefined
  let updatedDocuments = []

  let paginatedOpt = undefined;
  do {
    const result = await client.query(
      q.Map(
        q.Paginate(
          q.Range(q.Match(q.Index(collection.tsIndex)), after, null),
          paginatedOpt
        ),
        q.Lambda(
          ["ts", "ref"],
          q.Get(q.Var("ref"))
        )
      )
    )

    for(const document of result.data) {
      updatedDocuments.push(document)
      timestamp = Math.max(document.ts, timestamp)
    }
    paginatedOpt = result.after
  } while (!!paginatedOpt)

  return [latestTimestampOfEvent, updatedDocuments]
}

async function fetchDiffData(client, collection, {
  timestamp,
  data
}) {
  let newData = {...data};
  console.log(`Cache found from previous build. Fetching data since: ${timestamp} for collection ${collection.name}`)

  const [deletedResult, createdAndUpdatedResult] = await Promise.all([
    fetchDeletedRefsSinceTimestamp(client, collection, timestamp), 
    fetchCreatedAndUpdatedDocumentsSinceTimestamp(client, collection, timestamp)
  ])

  const [lastDeletedTimestamp, deletedRefs] = deletedResult
  const [lastCreatedOrUpdatedTimestamp, createdOrUpdatedDocuments] = createdAndUpdatedResult

  for(const deletedRef of deletedRefs) {
    delete newData[deletedRef]
  }

  for(const createdOrUpdatedDocument of createdOrUpdatedDocuments) {
    newData[getIdOfDocumentOrEvent(createdOrUpdatedDocument)] = createdOrUpdatedDocument
  }

  console.log(`Number of datapoints before update ${Object.values(data).length}`)
  console.log(`Number of datapoints after update ${Object.values(data).length}`)
  
  const newTimestamp = Math.max(lastDeletedTimestamp, lastCreatedOrUpdatedTimestamp, timestamp)
  // We are still returning the old data and timestamp because the delta fetching is not working yet.
  return [newTimestamp, newData]
}

async function fetchData(client, collection, cache) {
  const cacheKey = getCacheKey(collection)
  const value = await cache.get(cacheKey)
  const parsedCache = value ? parseJSON(value) : undefined;
  const [timestamp, data] = parsedCache ? await fetchDiffData(client, collection, parsedCache) : await fetchAllData(client, collection);
  
  if(timestamp && (!parsedCache || timestamp !== parsedCache.timestamp)) {
    await cache.set(cacheKey, toJSON({
      timestamp,
      data
    }));
  }
  return Object.values(data);
}

function getIdOfDocumentOrEvent(documentOrEvent) {
  return documentOrEvent.ref.id || documentOrEvent.ref["@ref"].id
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
    const id = getIdOfDocumentOrEvent(document);

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
      }

    }

    createNode(node);
  }
}