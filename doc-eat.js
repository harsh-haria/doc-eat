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

const analyzeJsonStructure = async (jsonData, fileName) => {
    try {
        // Generate a unique class name for this file
        const className = generateClassName(fileName);

        // Analyze the structure using OpenAI
        const structurePrompt = `
            Analyze the following JSON data and identify key fields that would be useful for querying:
            ${JSON.stringify(jsonData).substring(0, 4000)}

            Create a Weaviate schema with the following requirements:
            1. Include all top-level primitive fields (strings, numbers, booleans, dates)
            2. For nested objects, identify important fields that should be accessed directly
            3. For arrays, determine if they contain important data that should be searchable
            4. Include a "content" field to store the full JSON for retrieval

            Return a JSON object with the following structure:
            {
                "className": "${className}",
                "properties": [
                {"name": "fieldName", "dataType": ["string|number|boolean|date|text|string[]"]},
                ...
                ],
                "vectorIndexConfig": {
                "distance": "cosine"
                }
            }
        `;

        // Get schema recommendation from OpenAI
        const response = await openai.createCompletion({
            model: "gpt-3.5-turbo-instruct",
            prompt: structurePrompt,
            max_tokens: 800,
            temperature: 0.2,
        });

        // Parse the recommended schema
        const schemaRecommendation = JSON.parse(response.data.choices[0].text.trim());

        // Add additional fields we need
        const enhancedSchema = {
            ...schemaRecommendation,
            vectorizer: 'text2vec-openai',
            moduleConfig: {
                'text2vec-openai': {
                    model: 'ada',
                    modelVersion: '002',
                    type: 'text'
                }
            }
        };

        // Always ensure we have fileName and content fields
        if (!enhancedSchema.properties.find(p => p.name === 'fileName')) {
            enhancedSchema.properties.push({
                name: 'fileName',
                dataType: ['string']
            });
        }

        if (!enhancedSchema.properties.find(p => p.name === 'content')) {
            enhancedSchema.properties.push({
                name: 'content',
                dataType: ['text']
            });
        }

        // Store metadata about field types for future use
        const fieldTypes = {};
        enhancedSchema.properties.forEach(prop => {
            fieldTypes[prop.name] = prop.dataType[0];
        });

        // Store this schema information for later use
        fs.writeFileSync(
            path.join(__dirname, 'schemas', `${className}.json`),
            JSON.stringify({ schema: enhancedSchema, fieldTypes }, null, 2)
        );

        return { className, schema: enhancedSchema, fieldTypes };
    } catch (error) {
        console.error('Error analyzing JSON structure:', error);
        throw error;
    }
};

const createSchema = async (schema) => {
    try {
        // Check if schema already exists
        const schemaExists = await client.schema.classExist(schema.className);

        if (schemaExists) {
            console.log(`Schema ${schema.className} already exists.`);
            return;
        }

        // Create the schema
        await client.schema.classCreator().withClass(schema).do();
        console.log(`Schema ${schema.className} created successfully`);
    } catch (error) {
        console.error('Error creating schema:', error.message);
        throw error;
    }
};

const flattenJsonForIndexing = (data, prefix = '') => {
    const result = {};

    for (const [key, value] of Object.entries(data)) {
        const newKey = prefix ? `${prefix}.${key}` : key;

        if (value === null || value === undefined) {
            result[newKey] = '';
        } else if (typeof value === 'object' && !Array.isArray(value)) {
            // Recursively flatten nested objects
            Object.assign(result, flattenJsonForIndexing(value, newKey));
        } else if (Array.isArray(value)) {
            // For arrays, store as JSON string but also extract primitive values
            result[newKey] = JSON.stringify(value);

            // Also create individual entries for array items if they are primitive types
            if (value.length > 0 && typeof value[0] !== 'object') {
                result[`${newKey}Items`] = value;
            } else if (value.length > 0) {
                // For arrays of objects, flatten important fields
                value.forEach((item, index) => {
                    if (typeof item === 'object') {
                        for (const [itemKey, itemValue] of Object.entries(item)) {
                            if (typeof itemValue !== 'object') {
                                const itemArrayKey = `${newKey}[${index}].${itemKey}`;
                                result[itemArrayKey] = itemValue;
                            }
                        }
                    }
                });
            }
        } else {
            // Store primitive values directly
            result[newKey] = value;
        }
    }

    return result;
};

const ingestData = async (jsonData, fileName) => {
    try {
        // Make sure schemas directory exists
        if (!fs.existsSync(path.join(__dirname, 'schemas'))) {
            fs.mkdirSync(path.join(__dirname, 'schemas'));
        }

        // Analyze JSON structure and get schema
        const { className, schema, fieldTypes } = await analyzeJsonStructure(jsonData, fileName);

        // Create the schema in Weaviate
        await createSchema(schema);

        // Flatten the data for indexing
        const flattenedData = flattenJsonForIndexing(jsonData);

        // Prepare objects for batch insertion
        const objects = [];

        // If data is an array, create an object for each item
        if (Array.isArray(jsonData)) {
            for (let i = 0; i < jsonData.length; i++) {
                const item = jsonData[i];
                const flatItem = flattenJsonForIndexing(item);

                const properties = {
                    fileName: fileName,
                    content: JSON.stringify(item),
                    index: i,
                };

                // Add properties from the schema
                schema.properties.forEach(prop => {
                    if (prop.name !== 'fileName' && prop.name !== 'content' && prop.name !== 'index') {
                        if (flatItem[prop.name] !== undefined) {
                            properties[prop.name] = flatItem[prop.name];
                        }
                    }
                });

                objects.push({
                    class: className,
                    properties
                });
            }
        }
        else {
            // For single object, create one Weaviate object
            const properties = {
                fileName: fileName,
                content: JSON.stringify(jsonData),
            };

            // Add properties from the schema
            schema.properties.forEach(prop => {
                if (prop.name !== 'fileName' && prop.name !== 'content') {
                    if (flattenedData[prop.name] !== undefined) {
                        properties[prop.name] = flattenedData[prop.name];
                    }
                }
            });

            objects.push({
                class: className,
                properties
            });
        }

        // Batch import objects
        const batcher = client.batch.objectsBatcher();
        for (const obj of objects) {
            batcher.withObject(obj);
        }
        await batcher.do();

        console.log(`Successfully ingested data for ${fileName} as class ${className}`);
        return { success: true, className, objectsCount: objects.length };
    } catch (error) {
        console.error('Error ingesting data:', error);
        return { success: false, error: error.message };
    }
};

module.exports = {
    processDocument,
    promptAI,
    ingestData,
}