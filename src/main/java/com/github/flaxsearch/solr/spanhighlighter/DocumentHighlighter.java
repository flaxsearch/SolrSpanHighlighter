package com.github.flaxsearch.solr.spanhighlighter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.search.spans.SpanCollector;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class DocumentHighlighter {
    
    private static final Logger LOG = LoggerFactory.getLogger(DocumentHighlighter.class);

    IndexSchema schema;
    List<HighlightingTask> tasks;
    List<Pattern> hlFieldPatterns;
    
    public DocumentHighlighter(IndexSchema schema, List<HighlightingTask> tasks, String[] hlFields) {
        this.schema = schema;
        this.tasks = tasks;
        this.hlFieldPatterns = Arrays.stream(hlFields).map(x -> 
            Pattern.compile(x.replace("*",  ".*"))).collect(Collectors.toList());
    }
    
    public Map<String, List<String>> highlightDoc(Document doc) {
        
        // build a single-field, single-document, memory index
        MemoryIndex index = new MemoryIndex(true);
        for (IndexableField field : doc) {
            if (hlFieldPatterns.stream().anyMatch(x -> x.matcher(field.name()).matches())) {
                FieldType ft = schema.getFieldTypeByName(field.name());
                Analyzer analyzer = ft.getIndexAnalyzer();
                LOG.trace("field name={} value={}... analyzer={}", field.name(), field.stringValue().substring(0,  10), analyzer);
                if (analyzer != null && field.stringValue() != null) {
                    index.addField(field.name(), field.stringValue(), analyzer);
                }
            }
        }        
        index.freeze();

        IndexSearcher searcher = index.createSearcher();
        searcher.setQueryCache(null);
        
        // collect the matching spans for each of the highlighting tasks (in the same sorted collector)
        OffsetCollector collector = new OffsetCollector();
        for (HighlightingTask task : tasks) {
            LOG.debug("collecting spans for {}", task);
            collector.setTask(task);
            try {
                searcher.search(task.query, new SimpleCollector() {    
                    Scorer scorer;
    
                    @Override
                    public boolean needsScores() {
                        return true;
                    }
        
                    @Override
                    public void setScorer(Scorer scorer) throws IOException {
                        this.scorer = scorer;
                    }
        
                    @Override
                    public void collect(int doc) throws IOException {
                        SpanExtractor.collect(scorer, collector);
                    }
                });
            }
            catch (IOException e) {
                LOG.error("error collecting spans", e);
            }
        }
        
        // Now generate the highlighted text to return.
        Map<String, List<String>> results = new HashMap<>();
        
        for(Map.Entry<String, SortedSet<Offset>> entry : collector.offsets.entrySet()) {
            String field = entry.getKey();
            List<Offset> offsets = mergeOffsets(entry.getValue());
            List<String> fieldResults = new ArrayList<>(); 
            int fieldOffsetStart = 0;
            
            for (String fieldValue : doc.getValues(field)) {
                final int fieldOffsetEnd = fieldOffsetStart + fieldValue.length();
                        
                // find which, if any, offsets apply to this value (OPTIMIZE whole list each time is not efficient)
                final int fieldOffsetStart2 = fieldOffsetStart;     // for lambda
                List<Offset> currentOffsets = offsets.stream().filter(x -> 
                    x.start < fieldOffsetEnd && x.end >= fieldOffsetStart2).collect(Collectors.toList());
                
                if (currentOffsets.size() > 0) {
                    StringBuilder builder = new StringBuilder();
                    int hlOffsetEnd = 0;
                    for (Offset off : currentOffsets) {
                        // adjust offsets for current value. Allow offsets to overlap incompletely (FIXME is this necessary?)
                        int offStart = Math.max(0, off.start - fieldOffsetStart);
                        int offEnd = Math.min(fieldValue.length(), off.end - fieldOffsetStart);

                        builder.append(fieldValue, hlOffsetEnd, offStart);
                        builder.append(off.task.startTag);
                        builder.append(fieldValue, offStart, offEnd);
                        builder.append(off.task.endTag);
                        
                        hlOffsetEnd = offEnd;
                    }
                    builder.append(fieldValue, hlOffsetEnd, fieldValue.length());
                    fieldResults.add(builder.toString());
                }
                
                // the next field offset begins one after the current end
                fieldOffsetStart = fieldOffsetEnd + 1;
            }
            results.put(field, fieldResults);
        }

        return results;
    }
    
    /**
     * Merge a collection of offsets (which may overlap.)
     * @param offsets The offsets to merge, which are assumed to be sorted by the start offset.
     * @return The merged offsets.
     */
    protected List<Offset> mergeOffsets(Collection<Offset> offsets) {
        List<Offset> ret = new ArrayList<>(offsets.size());
        for (Offset off : offsets) {
            if (ret.isEmpty()) {
                ret.add(off);
            }
            else {
                int end = ret.size() - 1;
                if (ret.get(end).overlaps(off)) {
                    ret.set(end, ret.get(end).merge(off));
                }
                else {
                    ret.add(off);
                }
            }
        }
        return ret;
    }

    /**
     * Actually, a pair of offsets, which indicate where the "pre" and "post" highlighting tags should be inserted.
     * Also includes the highlighting task which generated this offset, so we know the priorities for merging.  
     */
    private static class Offset implements Comparable<Offset> {
        public final int start;
        public final int end;
        public final HighlightingTask task;
        
        public Offset(int start, int end, HighlightingTask task) {
            this.start = start;
            this.end = end;
            this.task = task;
        }
        
        @Override
        public int compareTo(Offset that) {
            return Integer.compare(this.start, that.start);
        }
        
        @Override
        public String toString() {
            return "Offset:"+ start + "-" + end;
        }
        
        /**
         * @return true, iff the supplied offset overlaps with this one.
         */
        public boolean overlaps(Offset that) {
            return this.end >= that.start && this.start <= that.end;
        }
        
        /**
         * @return a merged offset of this and the overlapping one
         */
        public Offset merge(Offset that) {
            HighlightingTask priorityTask = (this.task.priority < that.task.priority) ? this.task : that.task;
            return new Offset(Math.min(this.start, that.start), Math.max(this.end, that.end), priorityTask);
        }
    }

    /**
     * Convenience class for collecting offsets from a span query.
     */
    private static class OffsetCollector implements SpanCollector {
        Map<String, SortedSet<Offset>> offsets = new HashMap<>(); 
        private HighlightingTask currentTask;
        
        public void setTask(HighlightingTask task) {
            currentTask = task;
        }
        
        @Override
        public void collectLeaf(PostingsEnum postingsEnum, int i, Term term) throws IOException {
            String field =  term.field();
            if (offsets.containsKey(field) == false) {
                offsets.put(field, new TreeSet<>());
            }
            offsets.get(field).add(new Offset(postingsEnum.startOffset(), postingsEnum.endOffset(), currentTask));
        }
        
        @Override
        public void reset() { }        
    }    
}
