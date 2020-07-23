gatsby-source-faunadb

Source plugin for pulling data into gatsby from faunadb.
Will setup relationships between documents.


```js
module.exports = {
  //...
  plugins: [
    //...
    {
      resolve: `gatsby-source-fauna`,
      options: {
        // The secret for the key you're using to connect to your Fauna database.
        // You can generate on of these in the "Security" tab of your Fauna Console.
        secret: "___YOUR_FAUNADB_SECRET___",
        // Array of the collections you want to injest
        collections: ["collection1", "collection2"],
      },
    },
    //...
  ],
}
```