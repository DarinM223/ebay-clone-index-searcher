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

			//retrieve matching documents after skipping numResultsToSkip
			int curr = 0;
			for (int i = numResultsToSkip - 1; i < hits.length; i++) {
				Document doc = searcher.doc(hits[i].doc);
				String id = doc.get("id");
				String name = doc.get("name");
				results[curr] = new SearchResult(id, name);
				curr++;
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
