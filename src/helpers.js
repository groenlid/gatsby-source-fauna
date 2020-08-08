const { parseJSON, toJSON } = require('faunadb/src/_json')
const { query } = require('faunadb')
const q = query

export function getGatsbyType(collectionName) {
  return `faunadb${sanitizeName(collectionName)}`
}

export function sanitizeName(s) {
    return s.replace(/[^_a-zA-Z0-9]/g, ``).replace(/\b\w/g, l => l.toUpperCase())
}

async function fetchAllData(client, collection) {
    console.log(`No cache found from previous build. Fetching all data`)
    const data = { created: [], deleted: [] }

    let after = null
    let timestamp = null
    do {
        const paginatedOpt = after ? {
        after
        } : undefined
        const result = await client.query(q.Map(q.Paginate(q.Documents(q.Collection(collection.name)), paginatedOpt), q.Lambda(x => q.Get(x))))
        for(const document of result.data) {
          data.created.push(document)
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
  
async function fetchDiffData(client, collection, timestamp) {
    let newData = { created: [], deleted: [] };
    console.log(`Cache found from previous build. Fetching data since: ${timestamp} for collection ${collection.name}`)
  
    const [deletedResult, createdAndUpdatedResult] = await Promise.all([
      fetchDeletedRefsSinceTimestamp(client, collection, timestamp), 
      fetchCreatedAndUpdatedDocumentsSinceTimestamp(client, collection, timestamp)
    ])
  
    const [lastDeletedTimestamp, deletedRefs] = deletedResult
    const [lastCreatedOrUpdatedTimestamp, createdOrUpdatedDocuments] = createdAndUpdatedResult
  
    newData.deleted = deletedRefs;
    newData.created = createdOrUpdatedDocuments;
    
    const newTimestamp = Math.max(lastDeletedTimestamp, lastCreatedOrUpdatedTimestamp, timestamp)
    // We are still returning the old data and timestamp because the delta fetching is not working yet.
    return [newTimestamp, newData]
}

export function fetchData(client, collection, lastChangeTimestamp) {
    return lastChangeTimestamp ? fetchDiffData(client, collection, lastChangeTimestamp) : fetchAllData(client, collection)
}
  
export function getIdOfDocumentOrEvent(documentOrEvent) {
    return documentOrEvent.ref.id || documentOrEvent.ref["@ref"].id
}