const documentProcessor = require('../doc-eat');

exports.GetPromptResponse = async (req, res, next) => {
    try {
        let fileName = req.body.fileName;
        let prompt = req.body.prompt;
        if (!prompt || !fileName) {
            return res.status(400).json({ message: "Please provide a valid input prompt" });
        }

        const sanitizedFileName = fileName.split('.').slice(0, -1).join('_').replace(/[^a-zA-Z0-9]/g, '_');

        const promptResponse = await documentProcessor.promptAI(sanitizedFileName, prompt);

        if (promptResponse.status === 200) {
            return res.status(200).json({ status: 200, message: promptResponse.message, response: promptResponse.response, objects: promptResponse.relevantChunks });
        }

        return res.status(500).json({ status: 500, message: promptResponse.message });
    } catch (error) {
        console.error(error);
        return res.status(500).json({ status: 500, message: "There was an error while uploading your file. Please try again later." });
    }
}