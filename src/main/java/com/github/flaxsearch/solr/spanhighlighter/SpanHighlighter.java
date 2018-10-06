package com.github.flaxsearch.solr.spanhighlighter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.Query;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.highlight.SolrHighlighter;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocList;
import org.apache.solr.search.QParser;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.SyntaxError;
import org.apache.solr.util.plugin.PluginInfoInitialized;
import org.apache.solr.schema.FieldType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * FIXME
 */
public class SpanHighlighter extends SolrHighlighter implements PluginInfoInitialized {

    // a list of fields to highlight, either comma- or space-delimited 
    static final String HL_FL = "hl.fl";
    
    // what to insert just before a higlighted term
    static final String HL_TAG_PRE = "hl.tag.pre";    
    
    // what to insert just after a higlighted term    
    static final String HL_TAG_POST = "hl.tag.post";    
    
    // optional highlighting queries. If not supplied, the main search query will be used. Multiple queries must 
    // be specified as hl.q.0, hl.q.1, ..., with corresponding pre and post tags.
    static final String HL_TAG_Q = "hl.q";
    
    static final String HL_QPARSER = "hl.qparser";
        
    static final String DEFAULT_QPARSER = "lucene";
        
    private static final Logger LOG = LoggerFactory.getLogger(SpanHighlighter.class);

    @Override
    public NamedList<Object> doHighlighting(DocList docs, Query query, SolrQueryRequest req, String[] defaultFields)
            throws IOException {
        
        SolrIndexSearcher searcher = req.getSearcher();
        IndexSchema schema = searcher.getSchema();
        SolrParams reqParams = req.getParams();

        String hlTagPre = reqParams.get(HL_TAG_PRE, "<em>"); 
        String hlTagPost = reqParams.get(HL_TAG_POST, "</em>");
        String[] hlFields = reqParams.get(HL_FL, "").split("[\\s\\,]");
        String hlQParser = reqParams.get(HL_QPARSER, DEFAULT_QPARSER);
        
        // highlighting output goes here
        NamedList<Object> ret = new SimpleOrderedMap<>();
                
        List<HighlightingTask> tasks = new ArrayList<>();

        // highlight multiple queries?
        for (int i = 0; i < 100; i++) {
            String q = reqParams.get(String.format("%s.%d", HL_TAG_Q, i));
            if (q == null) break;
            String tagPre = reqParams.get(String.format("%s.%d", HL_TAG_PRE, i), hlTagPre);
            String tagPost = reqParams.get(String.format("%s.%d", HL_TAG_PRE, i), hlTagPost);

            try {
                QParser parser = QParser.getParser(q, hlQParser, req);
                Query mquery = parser.parse();
                Query rewritten = QueryRewriter.INSTANCE.rewrite(mquery);
                LOG.debug("query rewritten {} -> {}", mquery, rewritten);
                tasks.add(new HighlightingTask(i, rewritten, tagPre, tagPost));
            }
            catch (SyntaxError e) {
                LOG.error("could not parse query {} ({})", q, e);
            }
        }

        if (tasks.isEmpty()) {
            // highlight the single main query
            Query rewritten = QueryRewriter.INSTANCE.rewrite(query);
            LOG.debug("main query rewritten {} -> {}", query, rewritten);
            tasks = Arrays.asList(new HighlightingTask(0, rewritten, hlTagPre, hlTagPost));
        }

        DocumentHighlighter highlighter = new DocumentHighlighter(schema, tasks, hlFields);                

        // Loop through the documents from the result set, applying highlighting
        DocIterator it = docs.iterator();
        while (it.hasNext()) {
            int doc = it.nextDoc();
            Document document = searcher.doc(doc);
            String docId = schema.printableUniqueKey(document);
            ret.add(docId, highlighter.highlightDoc(document));            
        }
        
        return ret;
    }

    @Override
    public void init(PluginInfo info) {
    }    
}
