package com.fedextesting;

import com.datastax.driver.core.*;
import com.google.common.base.Stopwatch;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class SolrQueryNgramCaptureAverageTime {

    private static Cluster m_cluster;
    private static Session m_session;
    private static String m_clusterAddress = "";
    private static String m_keyspaceName = "";
    private static Integer m_numberOfQueries = 0;

    public static void main(String[] args){

        if (args.length != 3){
            System.out.println("You must pass in the following arguments to this program: cluster_address keyspace_name number_of_queries_to_run");
            System.out.println("For example: 54.202.105.70 customer 1000");
            return;
        } else {
            m_clusterAddress = args[0];
            m_keyspaceName = args[1];
            try{
                m_numberOfQueries = new Integer(args[2]);
            }
            catch(Exception e){
                System.out.println("Invalid value passed as number_of_queries_to_run.  Defaulting to 1,000");
                m_numberOfQueries = 1000;
            }
        }

        //set default values if not passed in as arguments
        if (m_clusterAddress == "") {
            m_clusterAddress = "54.202.105.70";
        }
        if (m_keyspaceName == ""){
            m_keyspaceName = "x";
        }
        if (m_numberOfQueries == 0){
            m_numberOfQueries = 1000;
        }

        InitializeClusterAndSession();

        List<TimingResult> timings = new ArrayList<>();

        Stopwatch stopwatch = Stopwatch.createUnstarted();

        //String solrQuery = "select * from " + m_keyspaceName + ".y where solr_query = 'crid: \"?\" AND crid_ngram: \"?\"';";
        //PreparedStatement preparedStatement = m_session.prepare(solrQuery);

        System.out.println("Executing " + m_numberOfQueries.toString() + " queries");
        System.out.println("If any query returns 0 results, a warning will be written to the screen");
        System.out.println();

        for(int i = 0; i < m_numberOfQueries; i++){
            //get a value from Cassandra
            String wholeTerm = getRandomCompanyNameValue();
            //System.out.println("Original Company Name: " + wholeTerm);

            wholeTerm = wholeTerm
                    .replace(" ", "") //taking out spaces because the indexer for ngram_5_5 takes them out
                    //.replace("-", "")
                    .replace("'", "") //taking out single quotes because the solr syntax parser blows up if I leave them in -- can't figure out how to escape them
                    //.replace(",", "")
            ;
            //System.out.println("Edited Company Name: " + wholeTerm);


            //if the company name is "Johnson and Sons", we want to:
            // a) drop the first and last characters,
            // b) and produce 5-digit ngrams based on the rest
            //   "ohnso", "n and"
            // and
            // c) produce an ngram based on the last five
            //   "d Son"

            Integer shortenedTermLength = (wholeTerm.length() > 1) ? wholeTerm.length() - 1 : 0;
            String shortenedTerm = wholeTerm.substring(1, shortenedTermLength);
            List<String> terms = new ArrayList<>();
            for (int j = 0; j < shortenedTermLength; j += 5){
                if ((j + 5) > (shortenedTermLength - 1)){
                    break;
                }
                String term = shortenedTerm.substring(j, j + 5);
                //escaping single quotes b/c I ran into syntax error when I pass them
                //might think about escaping this whole list: + - && || ! ( ) { } [ ] ^ " ~ * ? : \
                //http://lucene.apache.org/core/3_6_0/queryparsersyntax.html#Escaping%20Special%20Characters
                if (term.contains("'")){
                    term = term.replace("'", "");
                }
                terms.add(term);
            }
            String nextTerm = shortenedTerm.substring(shortenedTermLength - 5 - 1, shortenedTermLength - 1);
            if (!terms.contains(nextTerm)){
                if (nextTerm.contains("'")){
                    nextTerm = nextTerm.replace("'", "");
                }
                terms.add(nextTerm);
            }

            //put together Solr Query
            String solrQuery = "select \"documentId\", \"companyName\" " +
                               "from " + m_keyspaceName + ".contact " +
                               "where solr_query = '{ " +
                                   "\"q\":\"*:*\", " +
                                   "\"fq\":[ ";
            boolean firstItem = true;
            for (String term : terms) {
                if (firstItem) {
                    firstItem = false;
                } else {
                    solrQuery += ", ";
                }
                //using this {!field f=?} sytnax because it lets me pass spaces without any kind of escape character
                solrQuery +=       "\"{!field f=companyName_ngram_5_5}" + term + "\"";
            }
//            solrQuery +=       "\"{!cached=false cost=101}crid: " + shortenedTerm + "*\" ";
            solrQuery +=           "] }' " +
                               "limit 1;";
            System.out.println(solrQuery);

            //run query
            stopwatch.reset();
            stopwatch.start();
            ResultSet results = m_session.execute(solrQuery);
            Integer resultsSize = results.all().size();
            if (resultsSize == 0){
                System.out.println("WARNING: Results from NGram query: " + Integer.toString(resultsSize));
            }

            //capture timing
            stopwatch.stop();
            long millis = stopwatch.elapsed(TimeUnit.MILLISECONDS);
            timings.add(new TimingResult(wholeTerm, shortenedTerm, millis));

        }

        int timingCount = 0;
        int timingSum = 0;
        long maxMillis = 0;
        for (TimingResult tr : timings) {
            timingCount++;

            long millis = tr.getMilliseconds();
            timingSum += millis;

            if (millis > maxMillis){
                maxMillis = millis;
            }

        }

        double averageTiming = timingSum / timingCount;

        System.out.println();
        System.out.println("Queries Run: " + Integer.toString(timings.size()));
        System.out.println("Average Query Time: " + Double.toString(averageTiming) + " millisecond(s)");
        System.out.println("Max Query Time: " + Double.toString(maxMillis) + " millisecond(s)");

        //make sure the program exits
        System.exit(0);

    }

    private static void InitializeClusterAndSession(){

        m_cluster = new Cluster.Builder()
                .addContactPoints(m_clusterAddress)
                .build();

        m_session = m_cluster.connect(m_keyspaceName);
    }

    private static String getRandomCompanyNameValue(){

        SecureRandom random = new SecureRandom();
        long randomToken = random.nextLong();
        //System.out.println("Random Token generated: " + Long.toString(randomToken));

        //documentId is the partition key, so you have to pass that into the token function
        String cql = "select \"companyName\" from " + m_keyspaceName + ".contact where token(\"documentId\") > " + Long.toString(randomToken) + " limit 1;";
        ResultSet results = m_session.execute(cql);
        String companyName = results.one().getString("\"companyName\"");

//        int lowerLimit = 7;
//        int upperLimit = 10;
//        int randomLength = random.nextInt(upperLimit - lowerLimit) + lowerLimit;
//        if (crid.length() < randomLength) {
//            return crid;
//        } else {
//            return crid.substring(0, randomLength);
//        }
        return companyName;
    }

}
