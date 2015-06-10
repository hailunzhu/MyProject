package course;

import com.mongodb.BasicDBObject;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import static com.mongodb.client.model.Filters.eq;

import java.util.ArrayList;

import java.util.Date;
import java.util.List;

public class BlogPostDAO {
    MongoCollection<Document> postsCollection;

    public BlogPostDAO(final MongoDatabase blogDatabase) {
        postsCollection = blogDatabase.getCollection("posts");
    }

    // Return a single post corresponding to a permalink
    public Document findByPermalink(String permalink) {

        Document post = null;
        post =postsCollection.find(eq("permalink", permalink)).first();
        System.out.println("findby permalink");
        System.out.println(post);

        return post;
    }

    // Return a list of posts in descending order. Limit determines
    // how many posts are returned.
    public List<Document> findByDateDescending(int limit) {

        // Return a list of DBObjects, each one a post from the posts collection
        List<Document> posts = null;

        posts = postsCollection.find().sort(new BasicDBObject("date", -1)).limit(limit).into(new ArrayList<Document>());

        return posts;
    }


    public String addPost(String title, String body, List tags, String username) {

        System.out.println("inserting blog entry " + title + " " + body);

        String permalink = title.replaceAll("\\s", "_"); // whitespace becomes _
        permalink = permalink.replaceAll("\\W", ""); // get rid of non alphanumeric
        permalink = permalink.toLowerCase();

        // Build the post object and insert it
        Document post = new Document();
        List comments = new ArrayList();
        post.append("title", title)
                .append("body", body)
                .append("tags",tags)
                .append("author",username)
                .append("permalink",permalink)
                .append("comments",comments)
                .append("date",new Date());

        postsCollection.insertOne(post);


        return permalink;
    }

    // Append a comment to a blog post
    public void addPostComment(final String name, final String email, final String body,
                               final String permalink) {

        Document comment = new Document();

        comment.append("author",name)
                .append("body",body);
        if (email != null && !email.equals("")) {
            // the provided email address
           comment.append("email", email);
        }

        postsCollection.updateOne(eq("permalink", permalink), new Document("$push",new Document("comments",comment)));

    }
}
