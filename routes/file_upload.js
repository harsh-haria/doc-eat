const express = require('express');
const multer = require('multer');

const uploadPath = process.env.UPLOAD_PATH || './uploads';

const router = express.Router();

const storage = multer.diskStorage({
    destination: (req, file, cb) => {
        cb(null, uploadPath);
    },
    filename: (req, file, cb) => {
        cb(null, file.originalname);
    }
});

const upload = multer({ storage: storage });

const fileUploadController = require('../controllers/file_upload');

router.post('/uploadFile', upload.single('file'), fileUploadController.fileUpload);

module.exports = router;