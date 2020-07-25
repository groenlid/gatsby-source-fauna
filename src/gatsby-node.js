const { query, Client, values } = require('faunadb')

const q = query;

function sanitizeName(s) {
  return s.replace(/[^_a-zA-Z0-9]/g, ``).replace(/\b\w/g, l => l.toUpperCase())
}

exports.sourceNodes = async (
  sourceNodesOpt,
  pluginOptions
) => {

  const { secret, collections } = pluginOptions

  const client = new Client({ secret })

  try {

    await Promise.all(collections.map(collection => createNodes(sourceNodesOpt, client, collection)))

  } catch(err) {
    console.error(err)
  }

}

function createNodeIdImp(createNodeId, collection, id) {
  return createNodeId(`faunadb-${sanitizeName(collection)}-${id}`)
}

async function fetchAllData(client, collection) {
  console.log(`No cache found from previous build. Fetching all data`);
  const data = [];
  let after = null;

  do {
    const paginatedOpt = after ? {
      after
    } : undefined;
    const result = await client.query(q.Map(q.Paginate(q.Documents(q.Collection(collection)), paginatedOpt), q.Lambda(x => q.Get(x))));
    data.push(...result.data);
    after = result.after;
  } while (!!after);

  return data;
}

function getCacheKey(collection) {
  return `faunadb-${collection}`;
}

async function fetchDiffData(client, collection, {
  timestamp,
  data
}) {
  console.log(`Cache found from previous build. Fetching data since: ${new Date(timestamp)}`);
  return data;
}

async function fetchData(client, collection, cache) {
  const cacheKey = getCacheKey(collection);
  const timestamp = new Date().getTime();
  const value = await cache.get(cacheKey);
  const data = value ? await fetchDiffData(client, collection, value) : await fetchAllData(client, collection);
  const valueToCache = {
    timestamp,
    data
  };
  console.log(`Caching value: `, valueToCache);
  await cache.set(cacheKey, valueToCache);
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
      id: createNodeIdImp(createNodeId, collection, id),
      _id: id,
      _ts: document.ts,
      parent: null,
      children: [],
      internal: {
        type: `faunadb${sanitizeName(collection)}`,
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