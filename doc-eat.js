const fs = require('fs');
const path = require('path');
const pdfParse = require('pdf-parse');
const mammoth = require('mammoth');
const weaviate = require('weaviate-client');
const { getClient } = require('./weaviate');

// New helper function to read file based on extension
async function getSourceText(filePath) {
    const ext = path.extname(filePath).toLowerCase();
    if (ext === '.pdf') {
        const buffer = fs.readFileSync(filePath);
        const data = await pdfParse(buffer);
        return data.text;
    } else if (ext === '.docx') {
        const result = await mammoth.extractRawText({ path: filePath });
        return result.value;
    } else if (ext === '.json') {
        const content = fs.readFileSync(filePath, 'utf8');
        return content;
    } else if (ext === '.txt') {
        return fs.readFileSync(filePath, 'utf8');
    } else {
        throw new Error(`Unsupported file extension: ${ext}`);
    }
}

async function downloadAndChunk(filePath, chunkSize, overlapSize) {
    const finalFilePath = path.join(process.env.UPLOAD_PATH, filePath);
    const sourceText = await getSourceText(finalFilePath);
    const textWords = sourceText.replace(/\s+/g, ' ');
    // console.error(textWords);
    let chunks = [];
    for (let i = 0; i < textWords.length; i += chunkSize) {
        let chunk = textWords.slice(Math.max(i - overlapSize, 0), i + chunkSize);
        chunks.push(chunk);
    }
    return chunks;
}

async function getChunks(localFilePath) {
    const chunks = await downloadAndChunk(localFilePath, 150, 25);
    return chunks;
}

async function processDocument(inputFileName) {
    try {
        // establish connection to the db
        const WeaviateClient = await getClient();
        const file = inputFileName;
        const fileName = file.split('.').slice(0, -1).join('.');
        const chunkedData = await getChunks(file);

        // create schema and define the properties
        const schemaDefinition = {
            name: fileName,
            properties: [
                {
                    name: 'Chunk',
                    dataType: 'text'
                },
                {
                    name: 'chunk_index',
                    dataType: 'int'
                }
            ],
            vectorizers: weaviate.configure.vectorizer.text2VecOpenAI({
                model: 'text-embedding-3-small',
            }),
            generative: weaviate.configure.generative.openAI()
        }

        if (await WeaviateClient.collections.exists(fileName)) {
            await WeaviateClient.collections.delete(fileName);
        }

        const collection = await WeaviateClient.collections.create(schemaDefinition)

        // upload the chunks to the database
        // const collection = WeaviateClient.collections.get(fileName);
        const list = [];

        for (const index in chunkedData) {
            const obj = {
                properties: {
                    chunk: chunkedData[index],
                    chunk_index: +index,
                },
            };

            list.push(obj);
        }
        const result = await collection.data.insertMany(list)

        if (result.hasErrors) {
            return { status: 500, message: "Processing failed", error: result.errors };
        }

        return { status: 200, message: "Document processed successfully" };
    } catch (error) {
        console.error(error);
        return { status: 500, message: "Processing failed", error: error.message };
    }
}

async function promptAI(fileName, prompt) {
    const WeaviateClient = await getClient();

    // find collection with name fileName
    const collection = WeaviateClient.collections.get(fileName);

    // if no collection if found then return
    if (!collection) {
        return { status: 404, message: "Collection not found" };
    }

    const response = await collection.generate.fetchObjects({ groupedTask: prompt }, { limit: 5 });

    return { status: 200, message: "Response generated successfully", response: response.generated };
}

module.exports = {
    processDocument,
    promptAI
}