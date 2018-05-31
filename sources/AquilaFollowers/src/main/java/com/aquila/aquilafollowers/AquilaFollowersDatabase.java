/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.aquila.aquilafollowers;

import java.util.Map;
import org.jsoup.Jsoup;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.TransactionWork;
import static org.neo4j.driver.v1.Values.parameters;
import twitter4j.IDs;
import twitter4j.RateLimitStatus;
import twitter4j.Twitter;
import twitter4j.TwitterFactory;
import twitter4j.User;
import twitter4j.conf.ConfigurationBuilder;

/**
 * Adding followers.
 * @author Alexandre
 */
public class AquilaFollowersDatabase implements AutoCloseable {

    /**
     * Driver for connection.
     */
    private final Driver driver;

    /**
     * Class Constructor.
     * @param uri URI.
     * @param user Database username.
     * @param password Database password.
     */
    public AquilaFollowersDatabase(final String uri, final String user, final String password) {
        driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password));
    }

    /**
     * Override Closing driver.
     * @throws Exception Exception
     */
    @Override
    public final void close() throws Exception {
        driver.close();
    }

    /**
     * Adding User Node.
     * @param userID UserID of Twitter Account.
     * @param username Username of Twitter Account.
     * @param description Description of User Twitter Account.
     * @param nbfriends Number of friends of Twitter Account.
     * @param nbfollowers Number of followers of Twitter Account.
     * @param nbfavorites Number of favorites of Twitter Account.
     * @param verified Verified Twitter Account.
     * @param location Location of Twitter Account.
     */
    public final void addUserNode(final String userID, final String username, final String description, final String nbfriends, final String nbfollowers, final String nbfavorites, final String verified, final String location) {
        try (Session session = driver.session()) {
            String userNode = session
                    .writeTransaction(new TransactionWork<String>() {
                @Override
                public String execute(final Transaction tx) {
                    StatementResult result = tx.run(
                            "CREATE (u:User) "
                            + "SET u.UserID = $userID, "
                            + "u.Username = $username, "
                            + "u.Description = $description, "
                            + "u.NbFriends = $nbfriends, "
                            + "u.NbFollowers = $nbfollowers, "
                            + "u.NbFavoritesUser = $nbfavorites, "
                            + "u.Verified = $verified "
                            + "CREATE (l:Location) "
                            + "SET l.Location = $location "
                            + "WITH count(*) as dummy "
                            + "MATCH (u:User {UserID:  $userID}), (l:Location {Location: $location})"
                            + "MERGE (u)-[:FROM]->(l)"
                            + "RETURN l.location",
                            parameters("userID", userID, "username", username, "description", description, "nbfriends", nbfriends, "nbfollowers", nbfollowers, "nbfavorites", nbfavorites, "verified", verified, "location", location));
                    return result.next().get(0).asString();
                }
            });
        }
        catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    /**
     * Adding Following relationship between users.
     * @param mainaccount Main user Twitter Account.
     * @param user User Twitter Account.
     */
    public final void addFollowing(final String mainaccount, final String user) {
        try (Session session = driver.session()) {
            String userNode = session
                    .writeTransaction(new TransactionWork<String>() {
                @Override
                public String execute(final Transaction tx) {
                    StatementResult result = tx.run(
                            "MATCH (u1:User {UserID: $mainaccount}), (u2:User {UserID: $user}) "
                            + "MERGE (u1)<-[:FOLLOWS]-(u2) "
                            + "RETURN 'Friendship added'",
                            parameters("mainaccount", mainaccount, "user", user));
                    return result.single().get(0).asString();
                }
            });
        }
        catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    /**
     * Merging User nodes in the database.
     */
    public final void mergingUsers() {
        try (Session session = driver.session()) {
            String userNode = session
                    .writeTransaction(new TransactionWork<String>() {
                @Override
                public String execute(final Transaction tx) {
                    StatementResult result = tx.run(
                            "MATCH (u:User) "
                            + "WITH u.UserID as UserID, collect(u) AS nodes "
                            + "WHERE size(nodes) >  1 "
                            + "FOREACH (u in tail(nodes) | DETACH DELETE u) "
                            + "RETURN 'Users Merged'");
                    return result.next().get(0).asString();
                }
            });
        }
        catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    /**
     * Merging Location nodes in the database.
     */
    public final void mergingLocation() {
        try (Session session = driver.session()) {
            String userNode = session
                    .writeTransaction(new TransactionWork<String>() {
                @Override
                public String execute(final Transaction tx) {
                    StatementResult result = tx.run(
                            "MATCH (l:Location) "
                            + "WITH l.Location as Location, collect(l) AS nodes "
                            + "WHERE size(nodes) >  1 "
                            + "FOREACH (l in tail(nodes) | DETACH DELETE l) "
                            + "RETURN 'Location Merged'");
                    return result.next().get(0).asString();
                }
            });
        }
        catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws Exception {
        try (AquilaFollowersDatabase bd = new AquilaFollowersDatabase("bolt://localhost:7687", "neo4j", "123")) {

            //App Configuration in order to retrieve data
            ConfigurationBuilder cb = new ConfigurationBuilder();
            cb.setDebugEnabled(true)
                //Tianyuan
                /*.setOAuthConsumerKey("Ckt54ac8nVyqTMDlW4VALUaOE")
                .setOAuthConsumerSecret("sOOY3Dkujy0ruJLcMjJ1lA87mbqUgMG3K06GWwjHAEbyRSrmSU")
                .setOAuthAccessToken("708838056040734722-CZm6LvxAKs1trZMWCgnV6RqhpBTclMr")
                .setOAuthAccessTokenSecret("b98KWD1Tph2NCFtWhB6jJCQFYcW8OeiqtUXVqme9KRuRl");*/
                //Alexandre
                .setOAuthConsumerKey("qh7Nt1pGJHGvMt3DqNnjoacAZ")
                .setOAuthConsumerSecret("xJMhad585jycg4WrU2Oh6AJxQaTEMfKp3Xyn3X6U2p4dRE3InF")
                .setOAuthAccessToken("966009557372801025-KNpGkpGtSfPOU2iHzQFs9jLneDKA6on")
                .setOAuthAccessTokenSecret("UfBSEDaSTr5wDMBMweJJNPd3jBCiQr7HsSN3PSB0Utc57");
                //Colin
                /*.setOAuthConsumerKey("PrGFy40pU5ucfM15LpS73AU1c")
                .setOAuthConsumerSecret("hGbNAwIFyPzhm7VTkIrMb09fCd4pR864ekjHPuU5EKqWmedVHQ")
                .setOAuthAccessToken("965874428692324352-f3QHxWHRvVvZvLrqXknIKTTjfrpZCP7")
                .setOAuthAccessTokenSecret("zoI08QzLhAkK7fQKvcd8gWVVAOGsd8pIg9BBSxjONHSqu");*/

            // Connecting to Twitter
            TwitterFactory tf = new TwitterFactory(cb.build());
            Twitter twitter = tf.getInstance();

            User userEntry = twitter.showUser("EmmanuelMacron");
            // Variable for NodeEntry for Project Scope
            long nodeid;
            nodeid = userEntry.getId();

            //Adding some Followers to the database
            IDs followersid;
            long cursorfollow = -1;
            do {
                followersid = twitter.getFollowersIDs(nodeid, cursorfollow);
                for (long id : followersid.getIDs()) {
                    User follower = twitter.showUser(id);
                    if (follower.getStatus() != null) {
                        bd.addUserNode(String.valueOf(follower.getId()), String.valueOf(follower.getScreenName()), follower.getDescription(), String.valueOf(follower.getFriendsCount()), String.valueOf(follower.getFollowersCount()), String.valueOf(follower.getFavouritesCount()), String.valueOf(follower.isVerified()), String.valueOf(follower.getLocation()));
                        bd.addFollowing(String.valueOf(nodeid), String.valueOf(follower.getId()));
                        System.out.println("Follower Added: " + follower.getScreenName());
                        bd.mergingUsers();
                        bd.mergingLocation();
                        System.out.println("Merging Done");
                    }
                }
            } while ((cursorfollow = followersid.getNextCursor()) != 0); 
        }
    }
}
