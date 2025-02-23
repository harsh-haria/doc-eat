const documentProcessor = require('../doc-eat');

exports.fileUpload = async (req, res, next) => {
    try {
        if (!req.file) {
            return res.status(400).json({ message: "Please provide a valid file" });
        }

        let file = req.file.originalname;

        const response = await documentProcessor.processDocument(file);

        if (response.status === 200) {
            return res.status(200).json({ message: response.message });
        }
        else {
            return res.status(500).json({ message: response.message });
        }
    } catch (error) {
        console.error(error);
        return res.status(500).json({ message: "There was an error while uploading your file. Please try again later." });
    }
}