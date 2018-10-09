package com.github.flaxsearch.solr.spanhighlighter;

import org.junit.*;
import static org.mockito.Mockito.*;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.WildcardQuery;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;

import static org.assertj.core.api.Assertions.*;

public class testDocumentHighlighter {

    private static FieldType fieldType;
    private static IndexSchema schema;
    
    @BeforeClass
    public static void setup() {
        fieldType = mock(FieldType.class);
        when(fieldType.getIndexAnalyzer()).thenReturn(new StandardAnalyzer());
        
        schema = mock(IndexSchema.class);
        when(schema.getFieldTypeByName(any())).thenReturn(fieldType);
    }
    
    @Test
    public void testTermQuery() {
        Query query = new TermQuery(new Term("text", "banana"));
        Query rewritten = QueryRewriter.INSTANCE.rewrite(query);        
        List<HighlightingTask> tasks = Arrays.asList(new HighlightingTask(0, rewritten, "[", "]")); 
        
        Document doc = makeDoc("text", "what is my banana doing over there?");
        DocumentHighlighter highlighter = new DocumentHighlighter(schema, tasks, new String[] { "text" });

        Map<String, List<String>> results = highlighter.highlightDoc(doc);
        assertThat(results.keySet()).containsExactly("text");
        assertThat(results.get("text")).containsExactly("what is my [banana] doing over there?");
    }

    @Test
    public void testWildcard() {
        Query query = new WildcardQuery(new Term("text", "ban*"));
        Query rewritten = QueryRewriter.INSTANCE.rewrite(query);        
        List<HighlightingTask> tasks = Arrays.asList(new HighlightingTask(0, rewritten, "[", "]")); 
        
        Document doc = makeDoc("text", "what is my banana doing over there?");
        DocumentHighlighter highlighter = new DocumentHighlighter(schema, tasks, new String[] { "text" });

        Map<String, List<String>> results = highlighter.highlightDoc(doc);
        assertThat(results.keySet()).containsExactly("text");
        assertThat(results.get("text")).containsExactly("what is my [banana] doing over there?");
    }

    // FIXME test phrase and within queries
    
    @Test
    public void testAndQuery() throws ParseException {
        Query query = new QueryParser("text", new StandardAnalyzer()).parse("+banana +over");        
        Query rewritten = QueryRewriter.INSTANCE.rewrite(query);
        List<HighlightingTask> tasks = Arrays.asList(new HighlightingTask(0, rewritten, "[", "]")); 
        
        Document doc = makeDoc("text", "what is my banana doing over there?");
        DocumentHighlighter highlighter = new DocumentHighlighter(schema, tasks, new String[] { "text" });

        Map<String, List<String>> results = highlighter.highlightDoc(doc);
        assertThat(results.keySet()).containsExactly("text");
        assertThat(results.get("text")).containsExactly("what is my [banana] doing [over] there?");
    }

    @Test
    public void testOrQuery() throws ParseException {
        Query query = new QueryParser("text", new StandardAnalyzer()).parse("my banana is the best");        
        Query rewritten = QueryRewriter.INSTANCE.rewrite(query);
        List<HighlightingTask> tasks = Arrays.asList(new HighlightingTask(0, rewritten, "[", "]")); 
        
        Document doc = makeDoc("text", "what is my banana doing over there?");
        DocumentHighlighter highlighter = new DocumentHighlighter(schema, tasks, new String[] { "text" });

        Map<String, List<String>> results = highlighter.highlightDoc(doc);
        assertThat(results.keySet()).containsExactly("text");
        assertThat(results.get("text")).containsExactly("what is [my] [banana] doing over there?");
    }

    @Test
    public void testMultivaluedField() {
        Query query = new TermQuery(new Term("text", "banana"));
        Query rewritten = QueryRewriter.INSTANCE.rewrite(query);        
        List<HighlightingTask> tasks = Arrays.asList(new HighlightingTask(0, rewritten, "[", "]")); 
        
        Document doc = makeDoc("text", "What is my banana doing over there?",
                               "text", "First cut your banana into slices.",
                               "text", "Would you prefer a banana or an apple?");

        DocumentHighlighter highlighter = new DocumentHighlighter(schema, tasks, new String[] { "text" });

        Map<String, List<String>> results = highlighter.highlightDoc(doc);
        assertThat(results.keySet()).containsExactly("text");
        assertThat(results.get("text")).containsExactly(
                "What is my [banana] doing over there?",
                "First cut your [banana] into slices.",
                "Would you prefer a [banana] or an apple?"
        );
    }

    @Test
    public void testMultiFields() throws ParseException {
        Query query = new QueryParser("text", new StandardAnalyzer()).parse("band:war song:baby lyrics:belief");
        Query rewritten = QueryRewriter.INSTANCE.rewrite(query);        
        List<HighlightingTask> tasks = Arrays.asList(new HighlightingTask(0, rewritten, "[", "]")); 
        
        Document doc = makeDoc("band", "The War on Drugs",
                               "song", "Baby Missiles",
                               "lyrics", "I'm on the back of a new belief",
                               "album", "Slave Ambient");

        DocumentHighlighter highlighter = new DocumentHighlighter(schema, tasks, new String[] { "*" });

        Map<String, List<String>> results = highlighter.highlightDoc(doc);
        assertThat(results.keySet()).contains("band", "song", "lyrics");
        assertThat(results.get("band")).containsExactly("The [War] on Drugs");
        assertThat(results.get("song")).containsExactly("[Baby] Missiles");
        assertThat(results.get("lyrics")).containsExactly("I'm on the back of a new [belief]");
    }

    public static Document makeDoc(String... fields) {
        assert fields.length % 2 == 0;
        Document document = new Document();
        for (int i = 0; i < fields.length; i += 2) {
            String name = fields[i];
            String value = fields[i + 1];            
            document.add(new TextField(name, value, Field.Store.YES));
        }
        return document;
    }

}
