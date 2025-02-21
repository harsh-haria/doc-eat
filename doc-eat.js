const fs = require('fs');
const path = require('path');
const pdfParse = require('pdf-parse');
const mammoth = require('mammoth');
const weaviate = require('weaviate-client');
const dotenv = require('dotenv');

dotenv.config();

const weaviateUri = process.env.VECTOR_DB_URI;
const weaviateApiKey = process.env.VECTOR_DB_KEY;
const openAIKey = process.env.OPENAI_KEY;

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
    const sourceText = await getSourceText(filePath);
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

async function main(inputFileName, prompt) {
    try {
        // establish connection to the db
        const client = await weaviate.connectToWeaviateCloud(
            weaviateUri,
            {
                authCredentials: new weaviate.ApiKey(weaviateApiKey),
                headers: {
                    'X-OpenAI-Api-Key': openAIKey,
                }
            }
        )
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
            vectorizers: weaviate.configure.vectorizer.text2VecOpenAI(),
            generative: weaviate.configure.generative.openAI()
        }

        if (await client.collections.exists(fileName)) {
            await client.collections.delete(fileName);
        }

        const newCollection = await client.collections.create(schemaDefinition)
        // console.log('We have a new class!', newCollection['name']);

        // upload the chunks to the database
        const gitCollection = client.collections.get(fileName);
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
        const result = await gitCollection.data.insertMany(list)
        console.log('just bulk inserted', result);

        // insert the prompt and print the response
        const response = await gitCollection.generate.fetchObjects({ groupedTask: prompt }, { limit: 2 });
        console.log("AI: ", response.generated);
        return;
    } catch (error) {
        console.error(error);
    }
}

const fileName = 'Motive.pdf';
const prompt = `What is the text about? Is there any mention of profile links ?`;

main(fileName, prompt);