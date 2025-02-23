const fs = require('fs');
const path = require('path');
const pdfParse = require('pdf-parse');
const mammoth = require('mammoth');
const weaviate = require('weaviate-client');
const { OpenAI } = require('openai');
const { getClient } = require('./weaviate');

const openai = new OpenAI({ apiKey: process.env.OPENAI_KEY });

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

async function downloadAndChunk(fileName, chunkSize, overlapSize) {
    const finalFilePath = path.join(process.env.UPLOAD_PATH, fileName);
    const sourceText = await getSourceText(finalFilePath);
    const textWords = sourceText.replace(/\s+/g, ' ');
    // console.error(textWords);
    let chunks = [];
    for (let i = 0; i < textWords.length; i += chunkSize) {
        let chunk = textWords.slice(Math.max(i - overlapSize, 0), i + chunkSize);
        const vectors = await generateEmbeddings(chunk);
        chunks.push({
            chunk: chunk,
            vectors: vectors
        });
    }
    return chunks;
}

async function getChunks(localFilePath) {
    const chunks = await downloadAndChunk(localFilePath, 300, 75);
    return chunks;
}

async function generateEmbeddings(text) {
    const response = await openai.embeddings.create({
        model: 'text-embedding-ada-002',
        input: text,
    });
    return response.data[0].embedding;
}

async function processDocument(inputFileName) {
    try {
        // establish connection to the db
        const WeaviateClient = await getClient();
        const file = inputFileName;
        const fileName = file.split('.').slice(0, -1).join('_').replace(/[^a-zA-Z0-9]/g, '_');
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
                    chunk: chunkedData[index]['chunk'],
                    chunk_index: +index,
                },
                vectors: chunkedData[index]['vectors']
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

    if (!await WeaviateClient.collections.exists(fileName)) {
        return { status: 404, message: "Collection not found" };
    }

    const collection = WeaviateClient.collections.get(fileName);


    // Then generate response based on relevant chunks
    const response = await collection.generate.fetchObjects(
        { groupedTask: prompt },
        { limit: 5 },
        { returnReferences: true },
    );
    let objectsFetched = response.objects.map(item => { return { chunk: item.properties.chunk, chunk_index: item.properties.chunk_index, id: item.uuid } });

    return {
        status: 200,
        message: "Response generated successfully",
        response: response.generated,
        relevantChunks: objectsFetched
    };
}

module.exports = {
    processDocument,
    promptAI
}