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

function extractChunks(obj, parentKey = '') {
    let chunks = [];
    for (const [key, value] of Object.entries(obj)) {
        const fullKey = parentKey ? `${parentKey}.${key}` : key;
        if (typeof value === 'object' && value !== null) {
            chunks = chunks.concat(extractChunks(value, fullKey));
        } else {
            // Create a nested structure as required by Weaviate
            chunks.push({
                path: fullKey,
                content: String(value)
            });
        }
    }
    return chunks;
}

async function downloadAndChunk(fileName, extension, chunkSize, overlapSize) {
    const finalFilePath = path.join(process.env.UPLOAD_PATH, fileName);
    const sourceText = await getSourceText(finalFilePath);
    if (extension === '.json') {
        let vectorizedData = [];
        let jsonData;
        try {
            jsonData = JSON.parse(sourceText);
        } catch (error) {
            console.error(error);
            return { status: 500, message: "Error parsing JSON file" };
        }
        const chunks = extractChunks(jsonData);

        // Process each chunk with its path and content
        for (const [pos, chunk] of chunks.entries()) {
            const vectors = await generateEmbeddings(JSON.stringify(chunk));
            vectorizedData.push({
                chunk: chunk,  // This will be a nested object with path and content
                vectors: vectors
            });
        }
        return vectorizedData;
    }
    else {
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
}

async function getChunks(localFilePath, extension) {
    const chunks = await downloadAndChunk(localFilePath, extension, 300, 75);
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
        const extension = path.extname(inputFileName).toLowerCase();
        const chunkedData = await getChunks(file, extension);

        // Fixed schema definition with array format for nestedProperties
        let schemaDefinition = {
            name: fileName,
            properties: [
                {
                    name: 'chunk',
                    dataType: 'object',
                    nestedProperties: [
                        {
                            name: 'path',
                            dataType: 'text',
                            description: "The JSON path of the value"
                        },
                        {
                            name: 'content',
                            dataType: 'text',
                            description: "The actual content/value"
                        }
                    ]
                },
                {
                    name: 'chunk_index',
                    dataType: 'int'
                }
            ],
            vectorizer: "none",  // We'll provide our own vectors
            generative: weaviate.configure.generative.openAI()
        }

        // create a new variable with same name as the filename but the first character will be capital
        // this is because the collection name should start with a capital letter
        const collectionName = fileName.charAt(0).toUpperCase() + fileName.slice(1);

        if (await WeaviateClient.collections.exists(collectionName)) {
            await WeaviateClient.collections.delete(collectionName);
        }

        const collection = await WeaviateClient.collections.create(schemaDefinition)

        // upload the chunks to the database
        // const collection = WeaviateClient.collections.get(fileName);
        const list = [];

        for (const index in chunkedData) {
            const obj = {
                properties: {
                    chunk: chunkedData[index].chunk,  // This is now a nested object
                    chunk_index: +index,
                },
                vectors: chunkedData[index].vectors
            };

            list.push(obj);
        }
        const result = await collection.data.insertMany(list);

        const filePathToDelete = path.join(process.env.UPLOAD_PATH, file);
        fs.unlinkSync(filePathToDelete);

        if (result.hasErrors) {
            throw new Error(`Error inserting data: ${JSON.stringify(result.errors)}`);
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
    let objectsFetched = response.objects.map(
        item => {
            return {
                chunk: {
                    path: item.properties.chunk.path,
                    content: item.properties.chunk.content
                },
                chunk_index: item.properties.chunk_index,
                id: item.uuid
            }
        }
    );

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