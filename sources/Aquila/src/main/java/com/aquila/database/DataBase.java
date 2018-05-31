/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.aquila.database;

import java.util.ArrayList;
import org.jsoup.Jsoup;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.TransactionWork;
import static org.neo4j.driver.v1.Values.parameters;
import twitter4j.HashtagEntity;
import twitter4j.IDs;
import twitter4j.Paging;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterFactory;
import twitter4j.User;
import twitter4j.conf.ConfigurationBuilder;

/**
 * Database Class.
 * @author Aquila
 */
public class DataBase implements AutoCloseable {
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
    public DataBase(final String uri, final String user, final String password) {
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
     * Adding User index to the database.
     */
    public final void addIndexUser() {
        try (Session session = driver.session()) {
            String userNode = session
                    .writeTransaction(new TransactionWork<String>() {
                @Override
                public String execute(final Transaction tx) {
                    StatementResult result = tx.run(
                            "CREATE INDEX ON : User(Username)");
                    return result.next().get(0).asString();
                }
            });
        }
        catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    /**
     * Adding Location index to the database.
     */
    public final void addIndexLocation() {
        try (Session session = driver.session()) {
            String userNode = session
                    .writeTransaction(new TransactionWork<String>() {
                @Override
                public String execute(final Transaction tx) {
                    StatementResult result = tx.run(
                            "CREATE INDEX ON : Location(Location)");
                    return result.next().get(0).asString();
                }
            });
        }
        catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    /**
     * Adding Source index to the database.
     */
    public final void addIndexSource() {
        try (Session session = driver.session()) {
            String userNode = session
                    .writeTransaction(new TransactionWork<String>() {
                @Override
                public String execute(final Transaction tx) {
                    StatementResult result = tx.run(
                            "CREATE INDEX ON : Source(Source)");
                    return result.next().get(0).asString();
                }
            });
        }
        catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    /**
     * Adding  Tweet index to the database.
     */
    public final void addIndexTweet() {
        try (Session session = driver.session()) {
            String userNode = session
                    .writeTransaction(new TransactionWork<String>() {
                @Override
                public String execute(final Transaction tx) {
                    StatementResult result = tx.run(
                            "CREATE INDEX ON : Tweet(TweetID)");
                    return result.next().get(0).asString();
                }
            });
        }
        catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    /**
     * Adding Hashtag index to the database.
     */
    public final void addIndexHashtag() {
        try (Session session = driver.session()) {
            String userNode = session
                    .writeTransaction(new TransactionWork<String>() {
                @Override
                public String execute(final Transaction tx) {
                    StatementResult result = tx.run(
                            "CREATE INDEX ON : Hashtag(Hashtag)");
                    return result.next().get(0).asString();
                }
            });
        }
        catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    /**
     * Returning Source without URL.
     * @param source source of app
     * @return string.
     */
    public final static String getSource(final String source) {
         return Jsoup.parse(source).text();
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
     * Adding Friendship.
     * @param mainaccount Twitter Main Account.
     * @param user Friend ID.
     */
    public final void addFriendship(final String mainaccount, final String user) {
        try (Session session = driver.session()) {
            String userNode = session
                    .writeTransaction(new TransactionWork<String>() {
                @Override
                public String execute(final Transaction tx) {
                    StatementResult result = tx.run(
                            "MATCH (u1:User {UserID: $mainaccount}), (u2:User {UserID: $user}) "
                            + "MERGE (u1)<-[:FRIEND]-(u2) "
                            + "RETURN 'Friendship added'",
                            parameters("mainaccount", mainaccount, "user", user));
                    return result.next().get(0).asString();
                }
            });
        }
        catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    /**
     * Adding geocalised Tweets.
     * @param user user Twitter Account.
     * @param source App used for tweeting.
     * @param tweetid Tweet ID.
     * @param tweetcontent Tweet Content.
     * @param tweetdate Tweet Date.
     * @param nbretweets Number of retweets.
     * @param nbfavorites Number of favorites.
     * @param tweetlatitude Tweet Latitude.
     * @param tweetlongitude Tweet Longitude.
     */
    public final void addTweetGeocalised(final String user, final String source, final String tweetid, final String tweetcontent, final String tweetdate, final String nbretweets, final String nbfavorites, final String tweetlatitude, final String tweetlongitude) {
        try (Session session = driver.session()) {
            String userNode = session
                    .writeTransaction(new TransactionWork<String>() {
                @Override
                public String execute(final Transaction tx) {
                    StatementResult result = tx.run(
                            "CREATE (t:Tweet) "
                            + "SET t.TweetID = $tweetid, "
                            + "t.TweetContent = $tweetcontent, "
                            + "t.TweetDate = $tweetdate, "
                            + "t.NbRetweets = $nbretweets, "
                            + "t.NbFavorites = $nbfavorites, "
                            + "t.TweetLatitude = $tweetlatitude, "
                            + "t.TweetLongitude = $tweetlongitude "
                            + "CREATE (d:Date)"
                            + "SET d.Day = $day, "
                            + "d.Month = $month, "
                            + "d.Year = $year "
                            + "CREATE (s:Source) "
                            + "SET s.Source = $source "
                            + "WITH count(*) as dummy "
                            + "MATCH (t:Tweet {TweetID: $tweetid}), (s:Source {Source: $source}) "
                            + "MERGE (t)-[:HAS_SOURCE]->(s) "
                            + "WITH count(*) as dummy2 "
                            + "MATCH (t:Tweet {TweetID: $tweetid}), (u:User {UserID: $user}) "
                            + "MERGE (u)-[:TWEETED]->(t) "
                            + "WITH count(*) as dummy3 "
                            + "MATCH (t:Tweet {TweetID: $tweetid}), (d:Date {Day: $day, Month: $month, Year: $year}) "
                            + "MERGE (t)-[:DATED_OF]->(d) "
                            + "RETURN t.tweetid",
                            parameters("user", user, "source", source, "tweetid", tweetid, "tweetcontent", tweetcontent, "tweetdate", tweetdate, "day", getDay(tweetdate), "month", getMonth(tweetdate), "year", getYear(tweetdate), "nbretweets", nbretweets, "nbfavorites", nbfavorites, "tweetlatitude", tweetlatitude, "tweetlongitude", tweetlongitude));
                    return result.next().get(0).asString();
                }
            });
        }
        catch (Exception e) {
            System.out.println("Tweet Geocalised");
            System.out.println(e.getMessage());
        }
    }

    /**
     * Adding geocalised Retweets.
     * @param user user Twitter Account.
     * @param source App used for tweeting.
     * @param tweetid Tweet ID.
     * @param tweetcontent Tweet Content.
     * @param tweetdate Tweet Date.
     * @param nbretweets Number of retweets.
     * @param nbfavorites Number of favorites.
     * @param tweetlatitude Tweet Latitude.
     * @param tweetlongitude Tweet Longitude.
     */
    public final void addRetweetGeocalised(final String user, final String source, final String tweetid, final String tweetcontent, final String tweetdate, final String nbretweets, final String nbfavorites, final String tweetlatitude, final String tweetlongitude) {
        try (Session session = driver.session()) {
            String userNode = session
                    .writeTransaction(new TransactionWork<String>() {
                @Override
                public String execute(final Transaction tx) {
                    StatementResult result = tx.run(
                            "CREATE (t:Tweet) "
                            + "SET t.TweetID = $tweetid, "
                            + "t.TweetContent = $tweetcontent, "
                            + "t.TweetDate = $tweetdate, "
                            + "t.NbRetweets = $nbretweets, "
                            + "t.NbFavorites = $nbfavorites, "
                            + "t.TweetLatitude = $tweetlatitude, "
                            + "t.TweetLongitude = $tweetlongitude "
                            + "CREATE (d:Date)"
                            + "SET d.Day = $day, "
                            + "d.Month = $month, "
                            + "d.Year = $year "
                            + "CREATE (s:Source) "
                            + "SET s.Source = $source "
                            + "WITH count(*) as dummy "
                            + "MATCH (t:Tweet {TweetID: $tweetid}), (s:Source {Source: $source}) "
                            + "MERGE (t)-[:HAS_SOURCE]->(s) "
                            + "WITH count(*) as dummy2 "
                            + "MATCH (t:Tweet {TweetID: $tweetid}), (u:User {UserID: $user}) "
                            + "MERGE (u)-[:RETWEETED]->(t) "
                            + "WITH count(*) as dummy3 "
                            + "MATCH (t:Tweet {TweetID: $tweetid}), (d:Date {Day: $day, Month: $month, Year: $year}) "
                            + "MERGE (t)-[:DATED_OF]->(d) "
                            + "RETURN t.tweetid",
                            parameters("user", user, "source", source, "tweetid", tweetid, "tweetcontent", tweetcontent, "tweetdate", tweetdate, "day", getDay(tweetdate), "month", getMonth(tweetdate), "year", getYear(tweetdate), "nbretweets", nbretweets, "nbfavorites", nbfavorites, "tweetlatitude", tweetlatitude, "tweetlongitude", tweetlongitude));
                    return result.next().get(0).asString();
                }
            });
        }
        catch (Exception e) {
            System.out.println("Retweet Geocalised");
            System.out.println(e.getMessage());
        }
    }

    /**
     * Adding non Geocalised Tweets.
     * @param user user Twitter Account.
     * @param source App used for tweeting.
     * @param tweetid Tweet ID.
     * @param tweetcontent Tweet Content.
     * @param tweetdate Tweet Date.
     * @param nbretweets Number of retweets.
     * @param nbfavorites Number of favorites.
     */
    public final void addTweetNotGeocalised(final String user, final String source, final String tweetid, final String tweetcontent, final String tweetdate, final String nbretweets, final String nbfavorites) {
        try (Session session = driver.session()) {
            String userNode = session
                    .writeTransaction(new TransactionWork<String>() {
                @Override
                public String execute(final Transaction tx) {
                    StatementResult result = tx.run(
                            "CREATE (t:Tweet) "
                            + "SET t.TweetID = $tweetid, "
                            + "t.TweetContent = $tweetcontent, "
                            + "t.TweetDate = $tweetdate, "
                            + "t.NbRetweets = $nbretweets, "
                            + "t.NbFavorites = $nbfavorites "
                            + "CREATE (d:Date)"
                            + "SET d.Day = $day, "
                            + "d.Month = $month, "
                            + "d.Year = $year "
                            + "CREATE (s:Source) "
                            + "SET s.Source = $source "
                            + "WITH count(*) as dummy "
                            + "MATCH (t:Tweet {TweetID: $tweetid}), (s:Source {Source: $source}) "
                            + "MERGE (t)-[:HAS_SOURCE]->(s) "
                            + "WITH count(*) as dummy2 "
                            + "MATCH (t:Tweet {TweetID: $tweetid}), (u:User {UserID: $user}) "
                            + "MERGE (u)-[:TWEETED]->(t) "
                            + "WITH count(*) as dummy3 "
                            + "MATCH (t:Tweet {TweetID: $tweetid}), (d:Date {Day: $day, Month: $month, Year: $year}) "
                            + "MERGE (t)-[:DATED_OF]->(d) "
                            + "RETURN t.tweetid",
                            parameters("user", user, "source", source, "tweetid", tweetid, "tweetcontent", tweetcontent, "tweetdate", tweetdate, "day", getDay(tweetdate), "month", getMonth(tweetdate), "year", getYear(tweetdate), "nbretweets", nbretweets, "nbfavorites", nbfavorites));
                    return result.next().get(0).asString();
                }
            });
        }
        catch (Exception e) {
            System.out.println("Tweet Not Geocalised");
            System.out.println(e.getMessage());
        }
    }

    /**
     * Adding non Geocalised Retweets.
     * @param user user Twitter Account.
     * @param source App used for tweeting.
     * @param tweetid Tweet ID.
     * @param tweetcontent Tweet Content.
     * @param tweetdate Tweet Date.
     * @param nbretweets Number of retweets.
     * @param nbfavorites Number of favorites.
     */
    public final void addRetweetNotGeocalised(final String user, final String source, final String tweetid, final String tweetcontent, final String tweetdate, final String nbretweets, final String nbfavorites) {
        try (Session session = driver.session()) {
            String userNode = session
                    .writeTransaction(new TransactionWork<String>() {
                @Override
                public String execute(final Transaction tx) {
                    StatementResult result = tx.run(
                            "CREATE (t:Tweet) "
                            + "SET t.TweetID = $tweetid, "
                            + "t.TweetContent = $tweetcontent, "
                            + "t.TweetDate = $tweetdate, "
                            + "t.NbRetweets = $nbretweets, "
                            + "t.NbFavorites = $nbfavorites "
                            + "CREATE (d:Date)"
                            + "SET d.Day = $day, "
                            + "d.Month = $month, "
                            + "d.Year = $year "
                            + "CREATE (s:Source) "
                            + "SET s.Source = $source "
                            + "WITH count(*) as dummy "
                            + "MATCH (t:Tweet {TweetID: $tweetid}), (s:Source {Source: $source}) "
                            + "MERGE (t)-[:HAS_SOURCE]->(s) "
                            + "WITH count(*) as dummy2 "
                            + "MATCH (t:Tweet {TweetID: $tweetid}), (u:User {UserID: $user}) "
                            + "MERGE (u)-[:RETWEETED]->(t) "
                            + "WITH count(*) as dummy3 "
                            + "MATCH (t:Tweet {TweetID: $tweetid}), (d:Date {Day: $day, Month: $month, Year: $year}) "
                            + "MERGE (t)-[:DATED_OF]->(d) "
                            + "RETURN t.tweetid",
                            parameters("user", user, "source", source, "tweetid", tweetid, "tweetcontent", tweetcontent, "tweetdate", tweetdate, "day", getDay(tweetdate), "month", getMonth(tweetdate), "year", getYear(tweetdate), "nbretweets", nbretweets, "nbfavorites", nbfavorites));
                    return result.next().get(0).asString();
                }
            });
        }
        catch (Exception e) {
            System.out.println("Retweet Not Geocalised");
            System.out.println(e.getMessage());
        }
    }

    /**
     * Adding Hashtags.
     * @param tweetid Tweet ID.
     * @param hashtag Hashtag value.
     */
    public final void addHashTag(final String tweetid, final String hashtag) {
        try (Session session = driver.session()) {
            String userNode = session
                    .writeTransaction(new TransactionWork<String>() {
                @Override
                public String execute(final Transaction tx) {
                    StatementResult result = tx.run(
                            "CREATE (h:Hashtag) "
                            + "SET h.Hashtag = $hashtag "
                            + "WITH count(*) as dummy "
                            + "MATCH (t:Tweet {TweetID: $tweetid}), (h:Hashtag {Hashtag: $hashtag}) "
                            + "MERGE (t)-[:HAS_HASHTAG]->(h) "
                            + "RETURN h.hashtag",
                            parameters("tweetid", tweetid, "hashtag", hashtag));
                    return result.next().get(0).asString();
                }
            });
        }
        catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    /**
     * Day of a tweet.
     * @param date date of the tweet
     * @return the day
     */
    static String getDay(final String date) {
        return date.substring(8, 10);
    }

    /**
     * Month of a tweet.
     * @param date date of the tweet
     * @return the moth
     */
    static String getMonth(final String date) {
        return date.substring(4, 7);
    }

    /**
     * Year of a tweet.
     * @param date date of the tweet
     * @return the year
     */
    static String getYear(final String date) {
        if ("CET ".equals(date.substring(20, 23))) {
            return date.substring(23, 27);
        } else {
            return date.substring(24, 28);
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
     * Merging Source nodes in the database.
     */
    public final void mergingSource() {
        try (Session session = driver.session()) {
            String userNode = session
                    .writeTransaction(new TransactionWork<String>() {
                @Override
                public String execute(final Transaction tx) {
                    StatementResult result = tx.run(
                            "MATCH (s:Source) "
                            + "WITH s.Source as Source, collect(s) AS nodes "
                            + "WHERE size(nodes) >  1 "
                            + "FOREACH (s in tail(nodes) | DETACH DELETE s) "
                            + "RETURN 'Source Merged'");
                    return result.next().get(0).asString();
                }
            });
        }
        catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    /**
     * Merging Hashtag nodes in the database.
     */
    public final void mergingHashtag() {
        try (Session session = driver.session()) {
            String userNode = session
                    .writeTransaction(new TransactionWork<String>() {
                @Override
                public String execute(final Transaction tx) {
                    StatementResult result = tx.run(
                            "MATCH (h:Hashtag) "
                            + "WITH h.Hashtag as Hashtag, collect(h) AS nodes "
                            + "WHERE size(nodes) >  1 "
                            + "FOREACH (h in tail(nodes) | DETACH DELETE h) "
                            + "RETURN 'Hashtag Merged'");
                    return result.next().get(0).asString();
                }
            });
        }
        catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    /**
     * Merging Tweet nodes in the database.
     */
    public final void mergingTweets() {
        try (Session session = driver.session()) {
            String userNode = session
                    .writeTransaction(new TransactionWork<String>() {
                @Override
                public String execute(final Transaction tx) {
                    StatementResult result = tx.run(
                            "MATCH (t:Tweet) "
                            + "WITH t.TweetID as TweetID, collect(t) AS nodes "
                            + "WHERE size(nodes) >  1 "
                            + "FOREACH (t in tail(nodes) | DETACH DELETE t) "
                            + "RETURN 'Tweets Merged'");
                    return result.next().get(0).asString();
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
     * Merging Date nodes in the database.
     */
    public final void mergingDate() {
        try (Session session = driver.session()) {
            String userNode = session
                    .writeTransaction(new TransactionWork<String>() {
                @Override
                public String execute(final Transaction tx) {
                    StatementResult result = tx.run(
                            "MATCH (d:Date) "
                            + "WITH d.Day as Day, d.Month as Month, d.Year as Year, collect(d) AS nodes "
                            + "WHERE size(nodes) >  1 "
                            + "FOREACH (d in tail(nodes) | DETACH DELETE d) "
                            + "RETURN 'Date Merged'");
                    return result.next().get(0).asString();
                }
            });
        }
        catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    /**
     * Main application.
     * @param args arguments.
     * @throws Exception Exception
     */
    public static void main(final String... args) throws Exception {
        try (DataBase bd = new DataBase("bolt://localhost:7687", "neo4j", "123")) {

            //App Configuration in order to retrieve data
            ConfigurationBuilder cb = new ConfigurationBuilder();
            cb.setDebugEnabled(true)
                //Tianyuan
                /*.setOAuthConsumerKey("Ckt54ac8nVyqTMDlW4VALUaOE")
                .setOAuthConsumerSecret("sOOY3Dkujy0ruJLcMjJ1lA87mbqUgMG3K06GWwjHAEbyRSrmSU")
                .setOAuthAccessToken("708838056040734722-CZm6LvxAKs1trZMWCgnV6RqhpBTclMr")
                .setOAuthAccessTokenSecret("b98KWD1Tph2NCFtWhB6jJCQFYcW8OeiqtUXVqme9KRuRl");*/
                //Alexandre
                /*.setOAuthConsumerKey("qh7Nt1pGJHGvMt3DqNnjoacAZ")
                .setOAuthConsumerSecret("xJMhad585jycg4WrU2Oh6AJxQaTEMfKp3Xyn3X6U2p4dRE3InF")
                .setOAuthAccessToken("966009557372801025-KNpGkpGtSfPOU2iHzQFs9jLneDKA6on")
                .setOAuthAccessTokenSecret("UfBSEDaSTr5wDMBMweJJNPd3jBCiQr7HsSN3PSB0Utc57");*/
                //Colin
                .setOAuthConsumerKey("PrGFy40pU5ucfM15LpS73AU1c")
                .setOAuthConsumerSecret("hGbNAwIFyPzhm7VTkIrMb09fCd4pR864ekjHPuU5EKqWmedVHQ")
                .setOAuthAccessToken("965874428692324352-f3QHxWHRvVvZvLrqXknIKTTjfrpZCP7")
                .setOAuthAccessTokenSecret("zoI08QzLhAkK7fQKvcd8gWVVAOGsd8pIg9BBSxjONHSqu");

            // Connecting to Twitter
            TwitterFactory tf = new TwitterFactory(cb.build());
            Twitter twitter = tf.getInstance();

            // Adding indexes in the database
            bd.addIndexUser();
            bd.addIndexTweet();
            bd.addIndexHashtag();
            bd.addIndexSource();
            bd.addIndexLocation();

            // Adding User and Location
            User userEntry = twitter.showUser("EmmanuelMacron");
            bd.addUserNode(String.valueOf(userEntry.getId()), userEntry.getScreenName(), userEntry.getDescription(), String.valueOf(userEntry.getFriendsCount()), String.valueOf(userEntry.getFollowersCount()), String.valueOf(userEntry.getFavouritesCount()), String.valueOf(userEntry.isVerified()), String.valueOf(userEntry.getLocation()));

            // Adding Main Account Tweets
            ArrayList<Status> mainaccountstatuses = new ArrayList();
            Paging mainpaging = new Paging(20);
            mainaccountstatuses.addAll(twitter.getUserTimeline(userEntry.getScreenName(), mainpaging));
            for (Status maintweet : mainaccountstatuses) {
                // Adding only French Tweets
                if ("fr".equals(maintweet.getLang())) {
                    // Checking Location
                    if (maintweet.getGeoLocation() != null) {
                        bd.addTweetGeocalised(String.valueOf(userEntry.getId()), String.valueOf(getSource(maintweet.getSource())), String.valueOf(maintweet.getId()), String.valueOf(maintweet.getText()), String.valueOf(maintweet.getCreatedAt()), String.valueOf(maintweet.getRetweetCount()), String.valueOf(maintweet.getFavoriteCount()), String.valueOf(maintweet.getGeoLocation().getLatitude()), String.valueOf(maintweet.getGeoLocation().getLongitude()));
                    } else {
                        bd.addTweetNotGeocalised(String.valueOf(userEntry.getId()), String.valueOf(getSource(maintweet.getSource())), String.valueOf(maintweet.getId()), String.valueOf(maintweet.getText()), String.valueOf(maintweet.getCreatedAt()), String.valueOf(maintweet.getRetweetCount()), String.valueOf(maintweet.getFavoriteCount()));
                    }
                }

                // ArrayList for hashtags recovering
                ArrayList<String> hashtags = new ArrayList();
                for (HashtagEntity hashtag : maintweet.getHashtagEntities()) {
                    hashtags.add(hashtag.getText());
                }
                //Adding HashTag in the NEO4J Base
                for (String hashtagInTweet : hashtags) {
                    bd.addHashTag(String.valueOf(maintweet.getId()), hashtagInTweet);
                }
            }

            // Variable for NodeEntry for Project Scope
            long nodeid;
            nodeid = userEntry.getId();

            //Retrieving friends from a twitter account
            IDs ids;
            long cursor = -1;
            long compteurid = 0;
            do {
                ids = twitter.getFriendsIDs("EmmanuelMacron", cursor);
                for (long id : ids.getIDs()) {
                    User user = twitter.showUser(id);
                    if (user.getStatus() != null) {
                        // Adding User and Location
                        bd.addUserNode(String.valueOf(user.getId()), user.getScreenName(), user.getDescription(), String.valueOf(user.getFriendsCount()), String.valueOf(user.getFollowersCount()), String.valueOf(user.getFavouritesCount()), String.valueOf(user.isVerified()), String.valueOf(user.getLocation()));
                        // Adding Friendship with main User
                        bd.addFriendship(String.valueOf(nodeid), String.valueOf(user.getId()));

                        // Adding Tweets
                        ArrayList<Status> statuses = new ArrayList();
                        Paging page = new Paging(20);
                        statuses.addAll(twitter.getUserTimeline(user.getScreenName(), page));
                        for (Status tweet : statuses) {
                            // Adding only French Tweets
                            if ("fr".equals(tweet.getLang())) {
                                // Checking Location
                                if (tweet.getGeoLocation() != null) {
                                    // Checking Retweet Status
                                    if (tweet.isRetweet()) {
                                        bd.addRetweetGeocalised(String.valueOf(user.getId()), String.valueOf(getSource(tweet.getSource())), String.valueOf(tweet.getId()), String.valueOf(tweet.getText()), String.valueOf(tweet.getCreatedAt()), String.valueOf(tweet.getRetweetCount()), String.valueOf(tweet.getFavoriteCount()), String.valueOf(tweet.getGeoLocation().getLatitude()), String.valueOf(tweet.getGeoLocation().getLongitude()));
                                    } else {
                                    bd.addTweetGeocalised(String.valueOf(user.getId()), String.valueOf(getSource(tweet.getSource())), String.valueOf(tweet.getId()), String.valueOf(tweet.getText()), String.valueOf(tweet.getCreatedAt()), String.valueOf(tweet.getRetweetCount()), String.valueOf(tweet.getFavoriteCount()), String.valueOf(tweet.getGeoLocation().getLatitude()), String.valueOf(tweet.getGeoLocation().getLongitude()));
                                    }
                                } else {
                                     // Checking Retweet Status
                                    if (tweet.isRetweet()) {
                                        bd.addRetweetNotGeocalised(String.valueOf(user.getId()), String.valueOf(getSource(tweet.getSource())), String.valueOf(tweet.getId()), String.valueOf(tweet.getText()), String.valueOf(tweet.getCreatedAt()), String.valueOf(tweet.getRetweetCount()), String.valueOf(tweet.getFavoriteCount()));
                                    } else {
                                    bd.addTweetNotGeocalised(String.valueOf(user.getId()), String.valueOf(getSource(tweet.getSource())), String.valueOf(tweet.getId()), String.valueOf(tweet.getText()), String.valueOf(tweet.getCreatedAt()), String.valueOf(tweet.getRetweetCount()), String.valueOf(tweet.getFavoriteCount()));
                                    }
                                }
                            }

                            // ArrayList for hashtags recovering
                            ArrayList<String> hashtags = new ArrayList();
                            for (HashtagEntity hashtag : tweet.getHashtagEntities()) {
                                hashtags.add(hashtag.getText());
                            }
                            //Add HashTag in the NEO4J Base
                            for (String hashtagInTweet : hashtags) {
                                bd.addHashTag(String.valueOf(tweet.getId()), hashtagInTweet);
                            }
                        }
                        compteurid++;
                        // Merging Nodes
                        System.out.println("Ami : " + compteurid);
                        bd.mergingLocation();
                        bd.mergingSource();
                        bd.mergingHashtag();
                        bd.mergingTweets();
                        bd.mergingUsers();
                        bd.mergingDate();
                        System.out.println("Merging Done");
                    }
                }
            } while ((cursor = ids.getNextCursor()) != 0);
        }
    }
}