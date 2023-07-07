import java.io.IOException;
import java.net.URL;
import java.nio.file.*;

import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.bson.types.Binary;

public class ImageDownloader {
    public static void main(String[] args) {
        String imageUrl = "https://www.feiniaomy.com/zb_users/upload/2018/04/201804121523531830672006.png";
        String databaseName = "sync";
        String collectionName = "FLINK_SYNC:IMAGE_RESOURCE";

        try {
            // Download the image
            Path imagePath = downloadImage(imageUrl);

            // Store the image in MongoDB
            storeImageInMongoDB(imagePath, databaseName, collectionName);

            // Retrieve and download the image from MongoDB
            downloadImageFromMongoDB(databaseName, collectionName, "image.jpg");

            System.out.println("Process completed successfully.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static Path downloadImage(String imageUrl) throws IOException {
        URL url = new URL(imageUrl);
        Path tempFile = Files.createTempFile("image", ".jpg");
        Files.copy(url.openStream(), tempFile, StandardCopyOption.REPLACE_EXISTING);
        return tempFile;
    }

    private static void storeImageInMongoDB(Path imagePath, String databaseName, String collectionName) {
        try {
            // Connect to MongoDB
            com.mongodb.client.MongoClient mongoClient = MongoClients.create("mongodb://sync:sync3332141241211@www.fastsay.cn:27017/sync");
            MongoDatabase database = mongoClient.getDatabase(databaseName);
            MongoCollection<Document> collection = database.getCollection(collectionName);

            // Read the image file as bytes
            byte[] imageBytes = Files.readAllBytes(imagePath);

            // Create a document and insert the image into MongoDB
            Document document = new Document("image", imageBytes);
            collection.insertOne(document);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void downloadImageFromMongoDB(String databaseName, String collectionName, String fileName) {
        try {
            // Connect to MongoDB
            com.mongodb.client.MongoClient mongoClient = MongoClients.create("mongodb://sync:sync3332141241211@www.fastsay.cn:27017/sync");
            MongoDatabase database = mongoClient.getDatabase(databaseName);
            MongoCollection<Document> collection = database.getCollection(collectionName);

            // Retrieve the document with the image from MongoDB
            Document document = collection.find().first();

            if (document != null && document.containsKey("image")) {
                Binary imageBinary = (Binary) document.get("image");
                byte[] imageBytes = imageBinary.getData();

                // Write the image bytes to a file
                Path imageFile = Paths.get(fileName);
                Files.write(imageFile, imageBytes, StandardOpenOption.CREATE);
                System.out.println("Image downloaded from MongoDB: " + imageFile.toAbsolutePath());
            } else {
                System.out.println("Image not found in MongoDB.");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
