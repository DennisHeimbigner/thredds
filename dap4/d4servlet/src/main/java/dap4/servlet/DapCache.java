/* Copyright 2012, UCAR/Unidata.
   See the LICENSE file for more information. */

package dap4.servlet;

import dap4.cdm.dsp.CDMDSP;
import dap4.core.ce.CEConstraint;
import dap4.core.data.DSP;
import dap4.core.util.DapContext;
import dap4.core.util.DapException;
import dap4.dap4lib.FileDSP;
import dap4.dap4lib.HttpDSP;
import dap4.dap4lib.netcdf.Nc4DSP;
import ucar.nc2.NetcdfFile;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Provide an LRU cache of DSPs.
 * It is expected (for now) that this is only used on the server side.
 * The cache key is assumed to be the DSP object.
 * The cache is synchronized to avoid race conditions.
 * Note that we do not have to release because Java
 * uses garbage collection and entries will be purged
 * if the LRU cache is full.
 * Singleton class
 */

abstract public class DapCache
{

    //////////////////////////////////////////////////
    // Constants

    static final int MAXFILES = 100; // size of the cache

    static public final String MATCHMETHOD = "dspMatch";

    //////////////////////////////////////////////////
    // Types

    static protected class ExtMatch
    {
        String ext;
        Class dsp;

        public ExtMatch(String ext, Class dsp) {
            this.ext = ext;
            this.dsp = dsp;
        }
    }

    static protected class FragMatch
    {
        String key;
        String value;
        Class dsp;

        public FragMatch(String key, String value, Class dsp)
        {
            this.key = key;
            this.value = value;
            this.dsp = dsp;
        }
    }

    static protected class ProtocolMatch
    {
        String proto;
        String replace;
        Class dsp;

        public ProtocolMatch(String proto, String replace, Class dsp)
        {
            this.proto = proto;
            this.replace = replace;
            this.dsp = dsp;
        }
    }

    //////////////////////////////////////////////////
    // Static variables

    static ExtMatch[] match = new ExtMatch[]{
            new ExtMatch(".dmr", SynDSP.class),
            new ExtMatch(".syn", SynDSP.class),
            new ExtMatch(".nc", Nc4DSP.class),
            new ExtMatch(".hdf5", Nc4DSP.class),
            new ExtMatch(null, FileDSP.class) // default
    };

    static FragMatch[] frags = new FragMatch[]{
            new FragMatch("mode", "dap4", HttpDSP.class),
            new FragMatch("proto", "dap4", HttpDSP.class),
            new FragMatch("protocol", "dap4", HttpDSP.class),
            new FragMatch("dap4", null, HttpDSP.class),
    };

    static ProtocolMatch[] protos = new ProtocolMatch[]{
            new ProtocolMatch("dap4", "https", HttpDSP.class),
    };

    /**
     * Define an lru cache of known DSP objects: oldest first.
     */
    static protected List<DSP> lru = new ArrayList<DSP>();

    /**************************************************/
    /* Check cache */
    static protected DSP locateDSP(String location)
            throws IOException
    {
        int lrusize = lru.size();
        for(int i = lrusize - 1; i >= 0; i--) {
            DSP dsp = lru.get(i);
            String dsppath = dsp.getLocation();
            if(dsppath.equals(location)) {
                // move to the front of the queue to maintain LRU property
                lru.remove(i);
                lru.add(dsp);
                CEConstraint.release(lru.get(0).getDMR());
                return dsp;
            }
        }
        return null; /* no match found */
    }

    static void addDSP(DSP dsp)
            throws DapException
    {
        // If cache is full, remove oldest entry
        if(lru.size() == MAXFILES) {
            // make room
            lru.remove(0);
            CEConstraint.release(lru.get(0).getDMR());
        }
        lru.add(dsp);
    }


    /**************************************************/
    // Provide versions of open corresponding to the sourc type

    // Open a NetcdfFile
    static public synchronized DSP open(NetcdfFile ncfile, DapContext cxt)
            throws IOException
    {
        assert cxt != null && ncfile != null;
        DSP dsp = locateDSP(ncfile.getLocation());
        if(dsp == null) {
            // Convert to CDMDSP
            dsp = new CDMDSP().open(ncfile);
        }
        dsp.setContext(cxt);
        return dsp;
    }

    // Open a File
    static public synchronized DSP open(File file, DapContext cxt)
            throws IOException
    {
        assert cxt != null && file != null;
        // Do not use the cache for this
        // Choose the DSP based on the file extension
        String path = file.getPath();
        Class cldsp = null;
        for(ExtMatch em : match) {
            if(em.ext == null) {
                cldsp = em.dsp;
                break;
            }
            if(path.endsWith(em.ext)) cldsp = em.dsp;
        }
        if(cldsp == null)
            throw new DapException("Indeciperable file: " + file);
        DSP dsp = null;
        try {
            dsp = (DSP) cldsp.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new IOException("Class instance creation failed", e);
        }
        dsp.setContext(cxt);
        return dsp;
    }

    // Open a URL
    static public synchronized DSP open(URI uri, DapContext cxt)
            throws IOException
    {
        assert cxt != null && uri != null;
        DSP dsp = null;

        // If uri protocol is file, then try to open as file
        if(uri.getScheme().equals("file")) {
            return open(new File(uri.getPath()), cxt);
        }

        dsp = locateDSP(uri.toString());
        if(dsp == null) {
            // See if this is a DAP4 url
            Class cldsp = null;
            // Search the fragment list for markers
            String fragments = uri.getFragment();
            Map<String, String> fragmap = parsefragment(fragments);
            for(FragMatch fm : frags) {
                String values = fragmap.get(fm.key);
                if(values != null) {
                    if(fm.value == null) {
                        cldsp = fm.dsp; // singleton case
                    } else {// search for match
                        if(values.indexOf(fm.key) >= 0) {
                            cldsp = fm.dsp;
                        }
                    }
                    if(cldsp != null) break;
                }
            }
            if(cldsp == null)
                throw new DapException("Indeciperable URI: " + uri);
            try {
                dsp = (DSP)cldsp.newInstance();
            } catch (InstantiationException | IllegalAccessException e) {
                throw new IOException("Class instance creation failed", e);
            }
        }
        dsp.setContext(cxt);
        return dsp;
    }

    static synchronized public void flush() // for testing
            throws Exception
    {
        while(lru.size() > 0) {
            DSP dsp = lru.get(0);
            CEConstraint.release(dsp.getDMR());
            dsp.close();
            lru.remove(0);
        }
    }


    static protected Map<String, String>
    parsefragment(String fragments)
    {
        String[] pieces = fragments.split("[&]");
        Map<String, String> map = new HashMap<>();
        for(String p : pieces) {
            String[] pair = p.split("[=]");
            if(pair.length == 1) {
                map.put(pair[0].trim(), "");
            } else {
                map.put(pair[0].trim(), pair[1].trim());
            }
        }

        return map;
    }


} // DapCache
