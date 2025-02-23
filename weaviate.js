const weaviate = require('weaviate-client');
const dotenv = require('dotenv');
dotenv.config();

const weaviateUri = process.env.VECTOR_DB_URI;
const weaviateApiKey = process.env.VECTOR_DB_KEY;
const openAIKey = process.env.OPENAI_KEY;

let clientInstance = null;

async function initializeClient() {
    if (!clientInstance) {
        clientInstance = await weaviate.connectToWeaviateCloud(
            weaviateUri,
            {
                authCredentials: new weaviate.ApiKey(weaviateApiKey)
            }
        );
        console.log(await clientInstance.isReady());
        console.log('Weaviate client initialized');
    }
    return clientInstance;
}

module.exports = {
    getClient: initializeClient
};