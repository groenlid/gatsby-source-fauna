const { Client, values } = require('faunadb')
const { fetchData, getIdOfDocumentOrEvent, sanitizeName, getGatsbyType } = require('./helpers')

function faunaLogger (response) {
  console.log(`Faunadb query used ${response['responseHeaders']['x-read-ops']} read-ops that took ${response.endTime - response.startTime} ms`)
}

let lastChangeTimestamp = null;

exports.sourceNodes = async (
  sourceNodesOpt,
  pluginOptions
) => {

  const { secret, collections } = pluginOptions
  const { cache } = sourceNodesOpt
  const client = new Client({ secret, observer: faunaLogger})
  lastChangeTimestamp = await cache.get(`timestamp`)

  try {
    const newTimestamps = await Promise.all(collections.map(collection => createNodes(sourceNodesOpt, client, collection)))
    lastChangeTimestamp = Math.max(...newTimestamps)
  } catch(err) {
    console.error(err)
  }

}

function createNodeIdImp(createNodeId, collectionName, id) {
  return createNodeId(`faunadb-${sanitizeName(collectionName)}-${id}`)
}

async function createNodes({
  actions,
  getNode,
  createNodeId,
  hasNodeChanged,
  getNodesByType,
  createContentDigest,
  cache
}, client, collection) {
  const {
    createNode,
    touchNode
  } = actions;

  const type = getGatsbyType(collection.name)

  const [ timestamp, data ] = await fetchData(client, collection, lastChangeTimestamp)
  const { created, deleted } = data

  for (const node of getNodesByType(type)) {
    if(deleted.includes(node.id)) continue;
    touchNode({ nodeId: node.id })
  }

  for (const document of created) {
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
        type,
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
  return timestamp
}

exports.onPostBuild = async ({ cache }) => {
  if(!lastChangeTimestamp){
    return
  }
  // set a timestamp at the end of the build
  await cache.set(`timestamp`, lastChangeTimestamp)
}