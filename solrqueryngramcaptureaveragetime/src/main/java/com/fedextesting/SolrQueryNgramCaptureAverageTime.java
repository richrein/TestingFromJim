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
            System.out.println("For example: 54.201.21.89 x 1000");
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
            m_clusterAddress = "54.201.21.89";
        }
        if (m_keyspaceName == ""){
            m_keyspaceName = "x";
        }
        if (m_numberOfQueries == 0){
            m_numberOfQueries = 1000;
        }

        InitializeClusterAndSession();

        List<TimingResult> timings = new ArrayList<TimingResult>();

        Stopwatch stopwatch = Stopwatch.createUnstarted();

        //String solrQuery = "select * from " + m_keyspaceName + ".y where solr_query = 'crid: \"?\" AND crid_ngram: \"?\"';";
        //PreparedStatement preparedStatement = m_session.prepare(solrQuery);

        System.out.println("Executing " + m_numberOfQueries.toString() + " queries");
        System.out.println();

        for(int i = 0; i <= m_numberOfQueries; i++){
            //get a value from Cassandra
            String wholeTerm = getRandomTerm();
            //the whole term should always be 15 digits
            Integer shortendedSize = 13;
            String shortenedTerm = wholeTerm.substring(0, shortendedSize);
            String firstFive = wholeTerm.substring(0, 5);
            String secondFive = wholeTerm.substring(5, 10);
            String thirdThree = wholeTerm.substring(10, 13);

            //put together Solr Query
            String solrQuery = "select crid, addr1 from " + m_keyspaceName + ".y where solr_query = '{ \"q\":\"*:*\", \"fq\":[ \"{!cached=false cost=1}crid_ngram: " + firstFive + "\", \"{!cached=false cost=1}crid_ngram: " + secondFive + "\", \"{!cached=false cost=1}crid_ngram: " + thirdThree + "\", \"{!cached=false cost=101}crid: " + shortenedTerm + "*\" ] }' LIMIT 1;";
            System.out.println(solrQuery);

            //run query
            stopwatch.reset();
            stopwatch.start();
            ResultSet results = m_session.execute(solrQuery);
            //System.out.println("Results from NGram query: " + Integer.toString(results.all().size()));

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
        System.out.println("Queries Run: " + Double.toString(timings.size()));
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

    private static String getRandomTerm(){

        SecureRandom random = new SecureRandom();
        long randomToken = random.nextLong();
        //System.out.println("Random Token generated: " + Long.toString(randomToken));
        String cql = "select * from " + m_keyspaceName + ".y where token(crid) > " + Long.toString(randomToken) + " limit 1;";
        ResultSet results = m_session.execute(cql);
        String crid = results.one().getString("crid");

//        int lowerLimit = 7;
//        int upperLimit = 10;
//        int randomLength = random.nextInt(upperLimit - lowerLimit) + lowerLimit;
//        if (crid.length() < randomLength) {
//            return crid;
//        } else {
//            return crid.substring(0, randomLength);
//        }
        return crid;
    }

}
