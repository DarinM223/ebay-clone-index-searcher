package edu.ucla.cs.cs144;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.File;
import java.util.Date;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.List;
import java.text.SimpleDateFormat;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.lucene.document.Document;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

import edu.ucla.cs.cs144.DbManager;
import edu.ucla.cs.cs144.SearchRegion;
import edu.ucla.cs.cs144.SearchResult;

public class AuctionSearch implements IAuctionSearch {
	
	public SearchResult[] basicSearch(String query, int numResultsToSkip, 
			int numResultsToReturn) {
		SearchResult[] results = new SearchResult[numResultsToReturn];
		try {
			//create new instance of search engine
			IndexSearcher searcher = new IndexSearcher(DirectoryReader.open(FSDirectory.open(new File("/var/lib/lucene/ebay/"))));
			QueryParser parser = new QueryParser("content", new StandardAnalyzer());

			//retrieve top numResultsToSkip + numResultsToReturn results
			Query q = parser.parse(query);
			TopDocs topDocs = searcher.search(q, numResultsToSkip + numResultsToReturn);

			//obtain ScoreDoc array from docs
			ScoreDoc[] hits = topDocs.scoreDocs;

			//check if there are results to return
			if (hits.length < numResultsToSkip) {
				return new SearchResult[0];
			}

			//check if there are enough results to return after the skip
			if (hits.length - numResultsToSkip < numResultsToReturn) {
				results = new SearchResult[hits.length - numResultsToSkip];
			}

			//retrieve matching documents after skipping numResultsToSkip
			for (int i = 0; i < results.length; i++) {
				Document doc = searcher.doc(hits[i + numResultsToSkip].doc);
				String id = doc.get("id");
				String name = doc.get("name");
				results[i] = new SearchResult(id, name);
			}
		}
		catch (Exception e) {
			System.out.println(e);
		}

		return results;
	}

	public SearchResult[] spatialSearch(String query, SearchRegion region,
			int numResultsToSkip, int numResultsToReturn) {
		// TODO: Your code here!
		return new SearchResult[0];
	}

	public String getXMLDataForItemId(String itemId) {
		// TODO: Your code here!
		return "";
	}
	
	public String echo(String message) {
		return message;
	}

}
