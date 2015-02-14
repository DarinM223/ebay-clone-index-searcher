package edu.ucla.cs.cs144;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.File;
import java.util.Date;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.List;
import java.util.HashSet;
import java.text.SimpleDateFormat;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;

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

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import org.w3c.dom.*;
import javax.xml.transform.*;
import java.io.StringWriter;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

public class AuctionSearch implements IAuctionSearch {
	
	public SearchResult[] basicSearch(String query, int numResultsToSkip, 
			int numResultsToReturn) {
		SearchResult[] results = null;
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
			} else {
				results = new SearchResult[numResultsToReturn];
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
<<<<<<< HEAD
		SearchResult[] basicResultArr = basicSearch(query, 0, numResultsToSkip + numResultsToReturn);
=======
		SearchResult[] basicResultArr = basicSearch(query, 0, 99999999);
>>>>>>> origin/test_spatial
		SearchResult[] spatialResultArr = null;
		try {
			Connection conn = DbManager.getConnection(true);

			String lxly = region.getLx() + " " + region.getLy();
			String rxly = region.getRx() + " " + region.getLy();
			String rxry = region.getRx() + " " + region.getRy();
			String lxry = region.getLx() + " " + region.getRy();

			String polygonStr = lxly + ", " + rxly + ", " + rxry + ", " + lxry + ", " + lxly;

			String queryStr = "SELECT ItemID FROM ItemLocation WHERE MBRContains(GeomFromText('Polygon((" + polygonStr + "))'), Coord)";
			PreparedStatement q = conn.prepareStatement(queryStr);
			ResultSet result = q.executeQuery();
			HashSet<String> resultHash = new HashSet<>();

			while (result.next()) {
				resultHash.add(result.getString("ItemID"));
			}

			ArrayList<SearchResult> totalResults = new ArrayList<>();

			for (int i = 0; i < basicResultArr.length; i++) {
				String itemID = basicResultArr[i].getItemId();
				if (resultHash.contains(itemID)) { // if search result is inside the specific region, add to spatial results
					totalResults.add(basicResultArr[i]);
				}
			}

			int length = numResultsToReturn;

			if (totalResults.size() < numResultsToReturn + numResultsToSkip) {
				length = totalResults.size() - numResultsToSkip;
			}

			spatialResultArr = new SearchResult[length];

			for (int i = 0; i < length; i++) {
				spatialResultArr[i] = totalResults.get(numResultsToSkip+i);
			}

			conn.close(); // close the connection after done
		} catch (Exception e) {
			System.out.println(e);
		}

		return spatialResultArr;
	}

	public String getXMLDataForItemId(String itemId) {
		// TODO: Your code here!
		String resultXML = "";
		try {
			Connection conn = DbManager.getConnection(true);

			PreparedStatement query = conn.prepareStatement("SELECT * FROM Item WHERE ItemID = ?");
			query.setString(1, itemId);
			ResultSet result  = query.executeQuery();

			result.first(); // get the first instance of item (best match)

			if (result.getRow() != 0) {
				DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
				DocumentBuilder builder = factory.newDocumentBuilder();
				org.w3c.dom.Document doc = builder.newDocument();

				/*
				 * Retrieves data from result
				 * TODO: make sure that the data is properly encoded (like in currency format or in timestamp format)
				 */

				String userid = result.getString("Seller");
				String name = result.getString("Name");
				String currently = getCurrencyString(result.getFloat("Currently"));

				String buyprice = getCurrencyString(result.getFloat("Buy_Price"));

				String firstbid = getCurrencyString(result.getFloat("First_Bid"));

<<<<<<< HEAD
        		String numberOfBids = result.getString("Number_of_Bids");
=======
				String numberOfBids = result.getString("Number_of_Bids");
>>>>>>> origin/test_spatial

				String started = result.getString("Started"); 
				started = getTimeString(started);

				String ends = result.getString("Ends");
				ends = getTimeString(ends);

				String location = result.getString("Location");

				boolean hasLatitudeLongitude;
				String latitude = null, longitude = null;
				try {
					latitude = result.getString("Latitude");
					longitude = result.getString("Longitude");
					hasLatitudeLongitude = true;
				} catch (SQLException e) {
					hasLatitudeLongitude = false;
				}

				String country = result.getString("Country");
				String description = result.getString("Description");

				PreparedStatement query2 = conn.prepareStatement("SELECT * FROM Seller WHERE UserID = ?");
				PreparedStatement query3 = conn.prepareStatement("SELECT * FROM ItemCategory WHERE ItemID = ?");
				PreparedStatement query4 = conn.prepareStatement("SELECT COUNT(*) FROM Bid WHERE ItemID = ?");
				PreparedStatement query5 = conn.prepareStatement("SELECT * FROM Bid WHERE ItemID = ?");

				query2.setString(1, userid);
				query3.setString(1, itemId);
				query4.setString(1, itemId);
				query5.setString(1, itemId);

				result = query2.executeQuery();
				result.first();
				String rating = result.getString("Rating");

				result = query3.executeQuery();
				result.first();
				ArrayList<String> categories = new ArrayList<>();
				while (result.next()) {
					categories.add(result.getString("Category"));
				}

				//result = query4.executeQuery();
				//result.first();
				//String numberOFBids = result.getString("COUNT(*)");

				/*
				 * Builds DOM Tree
				 */

				Element root = doc.createElement("Item");
				root.setAttribute("ItemID", itemId);
				doc.appendChild(root);

				Element elementName = doc.createElement("Name");
				elementName.appendChild(doc.createTextNode(name));
				root.appendChild(elementName);

				for (String category : categories) {
					Element elementCategory = doc.createElement("Category");
					elementCategory.appendChild(doc.createTextNode(category));
					root.appendChild(elementCategory);
				}

				Element elementCurrently = doc.createElement("Currently");
				elementCurrently.appendChild(doc.createTextNode(currently));
				root.appendChild(elementCurrently);

				Element elementBuyPrice = doc.createElement("Buy_Price");
				elementBuyPrice.appendChild(doc.createTextNode(buyprice));
				root.appendChild(elementBuyPrice);

				Element elementFirstBid = doc.createElement("First_Bid");
				elementFirstBid.appendChild(doc.createTextNode(firstbid));
				root.appendChild(elementFirstBid);

				Element elementNumBids = doc.createElement("Number_of_Bids");
				elementNumBids.appendChild(doc.createTextNode(numberOfBids));
				root.appendChild(elementNumBids);

				Element elementBids = doc.createElement("Bids");
				result = query5.executeQuery();
				while (result.next()) {
					String bidUserID = result.getString("UserID");
					String bidTime = result.getString("Time");
					bidTime = getTimeString(bidTime);

					// TODO: Convert to currency format?
					String bidAmount = getCurrencyString(result.getFloat("Amount"));

					Element elementBid = doc.createElement("Bid");
					PreparedStatement queryBid = conn.prepareStatement("SELECT * FROM Bidder WHERE UserID = ?");
					queryBid.setString(1, bidUserID);

					ResultSet bidResult = queryBid.executeQuery();
					bidResult.first();

					String bidUserRating = bidResult.getString("Rating");
					String bidUserLocation = bidResult.getString("Location");
					String bidUserCountry = bidResult.getString("Country");

					Element elementBidUser = doc.createElement("Bidder");
					elementBidUser.setAttribute("Rating", bidUserRating);
					elementBidUser.setAttribute("UserID", bidUserID);

					Element elementBidLocation = doc.createElement("Location");
					elementBidLocation.appendChild(doc.createTextNode(bidUserLocation));
					elementBidUser.appendChild(elementBidLocation);

					Element elementBidCountry = doc.createElement("Country");
					elementBidCountry.appendChild(doc.createTextNode(bidUserCountry));
					elementBidUser.appendChild(elementBidCountry);

					elementBid.appendChild(elementBidUser);

					Element elementTime = doc.createElement("Time");
					elementTime.appendChild(doc.createTextNode(bidTime));
					elementBid.appendChild(elementTime);

					Element elementAmount = doc.createElement("Amount");	
					elementAmount.appendChild(doc.createTextNode(bidAmount));
					elementBid.appendChild(elementAmount);

					elementBids.appendChild(elementBid);
				}

				root.appendChild(elementBids);

				Element elementLocation = doc.createElement("Location");

				// TODO: I don't know what happens if only one attribute is not null like if longitude is null but latitude is not
				// I also don't know if this is the correct way to check if its null
				if (hasLatitudeLongitude && latitude != null && longitude != null) {
					elementLocation.setAttribute("Latitude", latitude);
					elementLocation.setAttribute("Longitude", longitude);
				}

				// TODO: I don't know if I have to escape characters b/c I'm using XML Docment builder instead of concating strings
				elementLocation.appendChild(doc.createTextNode(location));

				Element elementCountry = doc.createElement("Country");
				elementCountry.appendChild(doc.createTextNode(country));
				root.appendChild(elementCountry);

				Element elementStarted = doc.createElement("Started");
				elementStarted.appendChild(doc.createTextNode(started));
				root.appendChild(elementStarted);

				Element elementEnds = doc.createElement("Ends");	
				elementEnds.appendChild(doc.createTextNode(ends));
				root.appendChild(elementEnds);

				Element elementSeller = doc.createElement("Seller");	
				elementSeller.setAttribute("Rating", rating);
				elementSeller.setAttribute("UserID", userid);
				root.appendChild(elementSeller);

				TransformerFactory transFactory = TransformerFactory.newInstance();
				Transformer transformer = transFactory.newTransformer();
				// I don't know wtf this is lol
				transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "no");
				transformer.setOutputProperty(OutputKeys.METHOD, "xml");
				transformer.setOutputProperty(OutputKeys.INDENT, "yes");
				transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
				StringWriter writer = new StringWriter();
				transformer.transform(new DOMSource(doc), new StreamResult(writer));
				resultXML = writer.getBuffer().toString();
			}

			conn.close();
		} catch (Exception e) {
			System.out.println(e);
		}
		return resultXML;
	}

	private String getTimeString(String timestamp) {
		SimpleDateFormat input = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		SimpleDateFormat output = new SimpleDateFormat("MMM-dd-yy HH:mm:ss");
		StringBuffer stringBuffer = new StringBuffer();

		try {
			Date d = input.parse(timestamp);
			return "" + output.format(d);
		} catch (Exception e) {
			System.out.println(e);
			return "";
		} 
	}

	private String getCurrencyString(float num) {
		return String.format("$%.2f", num);
	}
	
	public String echo(String message) {
		return message;
	}

}
