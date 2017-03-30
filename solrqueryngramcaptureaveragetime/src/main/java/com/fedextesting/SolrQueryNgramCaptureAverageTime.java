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

    public static void main(String[] args){

        if (args.length != 2){
            System.out.println("You must pass in the following arguments to this program: cluster_address keyspace_name");
            System.out.println("For example: 54.201.21.89 x");
            return;
        } else {
            m_clusterAddress = args[0];
            m_keyspaceName = args[1];
        }

        //set default values if not passed in as arguments
        if (m_clusterAddress == "") {
            m_clusterAddress = "54.201.21.89";
        }
        if (m_keyspaceName == ""){
            m_keyspaceName = "x";
        }

        InitializeClusterAndSession();

        List<TimingResult> timings = new ArrayList<TimingResult>();

        Stopwatch stopwatch = Stopwatch.createUnstarted();

        //String solrQuery = "select * from " + m_keyspaceName + ".y where solr_query = 'crid: \"?\" AND crid_ngram: \"?\"';";
        //PreparedStatement preparedStatement = m_session.prepare(solrQuery);

        for(int i = 0; i <= 100; i++){
            //get a value from Cassandra
            String wholeTerm = getRandomTerm();
            String shortenedTerm = "";
            Integer shortendedSize = 5;
            if (wholeTerm.length() > shortendedSize){
                shortenedTerm = wholeTerm.substring(0, shortendedSize);
            } else {
                shortenedTerm = wholeTerm;
            }

            //put together Solr Query
            String solrQuery = "select crid, addr1 from " + m_keyspaceName + ".y where solr_query = 'crid: " + wholeTerm + "* AND crid_ngram: " + shortenedTerm + "' LIMIT 1;";
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

        int lowerLimit = 7;
        int upperLimit = 10;
        int randomLength = random.nextInt(upperLimit - lowerLimit) + lowerLimit;
        if (crid.length() < randomLength) {
            return crid;
        } else {
            return crid.substring(0, randomLength);
        }
    }

}
