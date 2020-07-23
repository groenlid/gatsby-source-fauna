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

async function createNodes({ actions, getNode, createNodeId, hasNodeChanged, createContentDigest }, client, collection){
  const { createNode } = actions
  let after = null;
  do {

    const paginatedOpt = after ? { after } : undefined;

    const result = await client.query(
      q.Map(
        q.Paginate(q.Documents(q.Collection(collection)), paginatedOpt),
        q.Lambda(x => q.Get(x))
      )
    )

    after = result.after;

    for(const document of result.data){
      const id = document.ref.id || document.ref["@ref"].id;
      if (document.data == null) {
        return;
      }
      
      const node = {
        ...document.data,
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

      for(const [key, value] of Object.entries(document.data)){
        if(value instanceof values.Ref){
          node[`${key}___NODE`] = createNodeIdImp(createNodeId, value.collection.value.id, value.value.id)
        }
          //console.log('Found a ref!!!')
      }
      createNode(node)
    }

  } while(!!after)

}