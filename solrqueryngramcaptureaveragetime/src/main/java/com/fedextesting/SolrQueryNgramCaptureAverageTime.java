package com.fedextesting;

import com.datastax.driver.core.*;
import com.google.common.base.Stopwatch;

import java.io.File;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import java.io.FileReader;
import java.io.BufferedReader;

public class SolrQueryNgramCaptureAverageTime {

    private static Cluster m_cluster;
    private static Session m_session;

    private static String m_clusterAddress = "";
    private static String m_keyspaceName = "";

    public static void main(String[] args) {

        Integer numberOfQueries;
        String subcommand;
        String filePath = "";

        if (args.length < 4) {
            System.out.println("You must pass in the following arguments to this program: cluster_address keyspace_name number_of_queries_to_run sub_command");
            System.out.println("For example: 54.202.105.70 customer 1000 companyName_ngram_5_5_middle");
            return;
        } else {
            m_clusterAddress = args[0];
            m_keyspaceName = args[1];
            try {
                numberOfQueries = new Integer(args[2]);
            } catch (Exception e) {
                System.out.println("Invalid value passed as number_of_queries_to_run.  Defaulting to 1,000");
                numberOfQueries = 1000;
            }
            subcommand = args[3];
            if (args.length > 4){
                filePath = args[4];
            }
        }

        //set default values if not passed in as arguments
        if (m_clusterAddress.equals("")) {
            m_clusterAddress = "54.202.105.70";
        }
        if (m_keyspaceName.equals("")) {
            m_keyspaceName = "customer";
        }
        if (numberOfQueries == 0) {
            numberOfQueries = 1000;
        }
        if (subcommand.equals("")) {
            subcommand = "companyName_ngram_5_5_middle";
        }
        if (filePath.equals("")) {
            filePath = "queries.txt";
        }

        InitializeClusterAndSession();

        List<TimingResult> timings = new ArrayList<>();

        Stopwatch stopwatch = Stopwatch.createUnstarted();

        System.out.println("Executing " + numberOfQueries.toString() + " queries");
        System.out.println("If any query returns 0 results, a warning will be written to the screen");
        System.out.println();

        if (subcommand.equals("read_queries_from_file")){

            List<String> queries = new ArrayList<>();
            try {
                File file = new File(filePath);
                BufferedReader br = new BufferedReader(new FileReader(file));
                String query;
                while((query=br.readLine()) != null){
                    queries.add(query);
                }
            } catch(Exception ex) {
                ex.printStackTrace();
            }

            for(String query : queries) {
                ExecuteQuery(query, stopwatch, timings, "");
            }

        } else {
            for (int i = 0; i < numberOfQueries; i++) {
                //get a value from Cassandra
                String wholeTerm = getRandomCompanyNameValue();
                System.out.println("Original Company Name: " + wholeTerm);

                String solrQuery;
                switch (subcommand) {
                    case "companyName_ngram_5_5_middle": {
                        solrQuery = companyName_ngram_5_5_middle(wholeTerm);
                        break;
                    }
                    case "companyName_ngram_3_5_middle": {
                        solrQuery = companyName_ngram_3_5_middle(wholeTerm);
                        break;
                    }
                    case "companyName_text_middle": {
                        solrQuery = companyName_text_middle(wholeTerm);
                        break;
                    }
                    default: {
                        solrQuery = "";
                        break;
                    }
                }

                if (solrQuery.equals("")){
                    continue;
                }

                ExecuteQuery(solrQuery, stopwatch, timings, wholeTerm);

            }
        }

        DisplayRunStatistics(timings);

        //make sure the program exits
        System.exit(0);

    }

    private static void ExecuteQuery(String solrQuery, Stopwatch stopwatch, List<TimingResult> timings, String term){
        System.out.println(solrQuery);

        stopwatch.reset();

        //run query with stopwatch
        stopwatch.start();
        ResultSet results = m_session.execute(solrQuery);
        stopwatch.stop();

        //capture timing
        long millis = stopwatch.elapsed(TimeUnit.MILLISECONDS);
        timings.add(new TimingResult(term, millis));

        Integer resultsSize = results.all().size();
        if (resultsSize == 0) {
            System.out.println("WARNING: ZERO results returned from query");
        }
    }

    private static void DisplayRunStatistics(List<TimingResult> timings) {
        int timingCount = 0;
        int timingSum = 0;
        long maxMillis = 0;
        for (TimingResult tr : timings) {
            timingCount++;

            long millis = tr.getMilliseconds();
            timingSum += millis;

            if (millis > maxMillis) {
                maxMillis = millis;
            }

        }

        double averageTiming = timingSum / timingCount;

        System.out.println();
        System.out.println("Queries Run: " + Integer.toString(timings.size()));
        System.out.println("Average Query Time: " + Double.toString(averageTiming) + " millisecond(s)");
        System.out.println("Max Query Time: " + Double.toString(maxMillis) + " millisecond(s)");
    }

    private static void InitializeClusterAndSession() {

        m_cluster = new Cluster.Builder()
                .addContactPoints(m_clusterAddress)
                .build();

        m_session = m_cluster.connect(m_keyspaceName);
    }

    private static String getRandomCompanyNameValue() {

        SecureRandom random = new SecureRandom();
        long randomToken = random.nextLong();
        //System.out.println("Random Token generated: " + Long.toString(randomToken));

        //documentId is the partition key, so you have to pass that into the token function
        String cql = "select \"companyName\" from " + m_keyspaceName + ".contact where token(\"documentId\") > " + Long.toString(randomToken) + " limit 1;";
        ResultSet results = m_session.execute(cql);
        return results.one().getString("\"companyName\"");

    }

    private static String companyName_ngram_5_5_middle(String wholeTerm) {

        if (wholeTerm == null){
            return null;
        }

        wholeTerm = wholeTerm
                .replace(" ", "") //taking out spaces because the indexer for ngram_5_5 takes them out
                .replace("'", "") //taking out single quotes because the solr syntax parser blows up if I leave them in -- can't figure out how to escape them
        ;
        //System.out.println("Edited Company Name: " + wholeTerm);

        //if the company name is "Johnson and Sons", we want to:
        // a) drop the spaces => JohnsonandSons
        // b) drop the first and last characters => ohnsonandSon
        // c) and produce 5-digit ngrams based on the rest
        //   "ohnso", "nandS"
        // and
        // d) produce an ngram based on the last five
        //   "ndSon"

        Integer shortenedTermLength = (wholeTerm.length() > 1) ? wholeTerm.length() - 1 : 0;
        String shortenedTerm = wholeTerm.substring(1, shortenedTermLength);
        List<String> terms = new ArrayList<>();
        for (int j = 0; j < shortenedTermLength; j += 5) {
            if ((j + 5) > (shortenedTermLength - 1)) {
                break;
            }
            String term = escapeSpecialSolrChars(shortenedTerm.substring(j, j + 5));
            terms.add(term);
        }
        String nextTerm = escapeSpecialSolrChars(shortenedTerm.substring(shortenedTermLength - 5 - 1, shortenedTermLength - 1));
        if (!terms.contains(nextTerm)) {
            terms.add(nextTerm);
        }

        //put together Solr Query
        StringBuilder sb = new StringBuilder();
        sb.append("select \"documentId\", \"companyName\" ");
        sb.append("from ");
        sb.append(m_keyspaceName);
        sb.append(".contact ");
        sb.append("where solr_query = '{ ");
        sb.append("\"q\":\"*:*\", ");
        sb.append("\"fq\":[ ");
        boolean firstItem = true;
        for (String term : terms) {
            if (firstItem) {
                firstItem = false;
            } else {
                sb.append(", ");
            }
            //using this {!field f=?} syntax because it lets me pass spaces without any kind of escape character
            sb.append("\"{!field f=companyName_ngram_5_5}");
            sb.append(term);
            sb.append("\"");
        }
        sb.append("] }' ");
        sb.append("limit 1;");

        return sb.toString();
    }

    private static String companyName_ngram_3_5_middle(String wholeTerm) {

        if (wholeTerm == null){
            return null;
        }

        wholeTerm = wholeTerm
                .replace(" ", "") //taking out spaces because the indexer for ngram_3_5 takes them out
                .replace("'", "") //taking out single quotes because the solr syntax parser blows up if I leave them in -- can't figure out how to escape them
        ;
        //System.out.println("Edited Company Name: " + wholeTerm);

        //if the company name is "Johnson and Sons", we want to:
        // a) drop the spaces => JohnsonandSons
        // b) drop the first and last characters => ohnsonandSon
        // c) and produce 5-digit ngrams based on the rest
        //   "ohnso", "nandS"
        // and
        // d) produce an ngram based on the last five
        //   "ndSon"

        Integer shortenedTermLength = (wholeTerm.length() > 1) ? wholeTerm.length() - 1 : 0;
        String shortenedTerm = wholeTerm.substring(1, shortenedTermLength);

        List<String> terms = new ArrayList<>();
        for (int j = 0; j < shortenedTermLength; j += 5) {
            if ((j + 5) > (shortenedTermLength - 1)) {
                break;
            }
            String term = escapeSpecialSolrChars(shortenedTerm.substring(j, j + 5));
            terms.add(term);
        }
        String nextTerm = escapeSpecialSolrChars(shortenedTerm.substring(shortenedTermLength - 5 - 1, shortenedTermLength - 1));
        if (!terms.contains(nextTerm)) {
            terms.add(nextTerm);
        }

        //put together Solr Query
        StringBuilder sb = new StringBuilder();
        sb.append("select \"documentId\", \"companyName\" ");
        sb.append("from ");
        sb.append(m_keyspaceName);
        sb.append(".contact ");
        sb.append("where solr_query = '{ ");
        sb.append("\"q\":\"*:*\", ");
        sb.append("\"fq\":[ ");
        boolean firstItem = true;
        for (String term : terms) {
            if (firstItem) {
                firstItem = false;
            } else {
                sb.append(", ");
            }
            sb.append("\"{!cached=false cost=1}companyName_ngram:");
            sb.append(term);
            sb.append("\"");
        }
        sb.append("] }' ");
        sb.append("limit 1;");

        return sb.toString();
    }

    private static String companyName_text_middle(String wholeTerm) {

        if (wholeTerm == null){
            return null;
        }

        wholeTerm = wholeTerm
                .replace("'", "") //taking out single quotes because the solr syntax parser blows up if I leave them in -- can't figure out how to escape them
        ;
        //System.out.println("Edited Company Name: " + wholeTerm);

        //split on punctuation or whitespace (except single quotes -- like O'Reilly)
        //String[] originalTerms = wholeTerm.split("(?!\')\\p{Punct}");
        //String[] originalTerms = wholeTerm.split("[\\p{Punct}\\s]+");
        String[] originalTerms = wholeTerm.split("\\W+");
        List<String> terms = new ArrayList<>();
        for (String term : originalTerms) {
            term = escapeSpecialSolrChars(term);
            if (!terms.contains(term)){
                terms.add(term);
            }
        }

        //put together Solr Query
        StringBuilder sb = new StringBuilder();
        sb.append("select \"documentId\", \"companyName\" ");
        sb.append("from ");
        sb.append(m_keyspaceName);
        sb.append(".contact ");
        sb.append("where solr_query = '{ ");
        sb.append("\"q\":\"*:*\", ");
        sb.append("\"fq\":[ ");
        boolean firstItem = true;
        for (String term : terms) {
            if (firstItem) {
                firstItem = false;
            } else {
                sb.append(", ");
            }
            sb.append("\"{!field f=companyName_text}*");
            sb.append(term);
            sb.append("*\"");
        }
        sb.append("] }' ");
        sb.append("limit 1;");

        return sb.toString();
    }

    private static String escapeSpecialSolrChars(String term){
        //special characters list: + - && || ! ( ) { } [ ] ^ " ~ * ? : \
        //   http://lucene.apache.org/core/3_6_0/queryparsersyntax.html#Escaping%20Special%20Characters

        term = term.replace("\\", "\\\\\\"); //you gotta do this one first
        term = term.replace("+", "\\\\+");
        term = term.replace("-", "\\\\-");
        term = term.replace("&&", "\\\\&&");
        term = term.replace("||", "\\\\||");
        term = term.replace("!", "\\\\!");
        term = term.replace("(", "\\\\(");
        term = term.replace(")", "\\\\)");
        term = term.replace("{", "\\\\{");
        term = term.replace("}", "\\\\}");
        term = term.replace("[", "\\\\[");
        term = term.replace("]", "\\\\]");
        term = term.replace("^", "\\\\^");
        term = term.replace("\"", "\\\\\"");
        term = term.replace("~", "\\\\~");
        term = term.replace("*", "\\\\*");
        term = term.replace("?", "\\\\?");
        term = term.replace(":", "\\\\:");

        return term;

    }

}
