/* Copyright 2012, UCAR/Unidata.
   See the LICENSE file for more information. */

package dap4.servlet;

import dap4.cdm.dsp.CDMDSP;
import dap4.core.ce.CEConstraint;
import dap4.core.data.DSP;
import dap4.core.util.DapContext;
import dap4.core.util.DapException;
import dap4.dap4lib.AbstractDSP;
import dap4.dap4lib.DapCodes;
import dap4.dap4lib.FileDSP;
import dap4.dap4lib.HttpDSP;
import dap4.dap4lib.netcdf.Nc4DSP;
import ucar.nc2.NetcdfFile;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;


/**
 * On the server side, the input can come from a variety of sources,
 * including NetcdfFile, NetcdfDataset, synthetic sources (.syn), and
 * raw capturess of DAP4 on-the-wire captures (.raw).
 * This is intended to be hidden behind the facade of a DSP/AbstractDSP.
 * This facade can be very complex (CDMDSP that converts a CDM API to DAP4)
 * or relatively simple for .raw files.
 * Given the path to a source, or a NetcdfFile, this singleton class
 * figures out the appropriate DSP wrapper for it.
 * So it manages the mapping of various kinds of input sources
 * to the proper DSP to process that kind of source.
 * It is expected (for now) that this is only used on the server side.
 */

abstract public class DapDSP
{

    //////////////////////////////////////////////////
    // Constants

    static final String driveletters = "abcdefghijklmnopqrstuvwxyz" +"abcdefghijklmnopqrstuvwxyz".toUpperCase();

    //////////////////////////////////////////////////
    // Types

    static protected class ExtMatch // match by extension
    {
        String ext;
        Class dsp;

        public ExtMatch(String ext, Class dsp) {
            this.ext = ext;
            this.dsp = dsp;
        }
    }

    static protected class FragMatch // match by URL fragment keys
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

    static protected class ProtocolMatch // match by URL protocol
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

    /**************************************************/
    // Provide versions of open corresponding to the source type
    static protected DSP open(NetcdfFile ncfile, DapContext cxt)
            throws IOException
    {
        // Convert to CDMDSP
        CDMDSP dsp = new CDMDSP();
        if(dsp != null) dsp.setContext(cxt);
        dsp.open(ncfile);
        return dsp;
    }

    // Open a File
    static protected DSP open(File file, DapContext cxt)
            throws IOException
    {
        // Choose the DSP based on the file extension
        String path = file.getPath();
        Class cldsp = null;
        for(ExtMatch em : match) {
            if(em.ext == null) {
                cldsp = em.dsp;
                break;
            }
            if(path.endsWith(em.ext)) {cldsp = em.dsp; break;}
        }
        if(cldsp == null)
            throw new DapException("Indeciperable file: " + file);
        AbstractDSP dsp = null;
        try {
            dsp = (AbstractDSP) cldsp.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new IOException("Class instance creation failed", e);
        }
        if(dsp != null) dsp.setContext(cxt);
        dsp.open(file);
        return dsp;
    }

    // Open a URL
    static protected DSP open(URI uri, DapContext cxt)
            throws IOException
    {
        AbstractDSP dsp = null;
        // See if this is a DAP4 url
        Class cldsp = null;
        // Search the fragment list for markers
        String fragments = uri.getFragment();
        Map<String, String> fragmap = parsefragment(fragments);
        for(FragMatch fm : frags) {
            String values = fragmap.get(fm.key);
            if(values != null) {
                if(fm.value == null) {
                    cldsp = fm.dsp;
                    break;
                } // singleton case
                else {// search for match
                    if(values.indexOf(fm.key) >= 0) {
                        cldsp = fm.dsp;
                        break;
                    }
                }
            }
            if(cldsp == null)
                throw new DapException("Indeciperable URI: " + uri);
            try {
                dsp = (AbstractDSP) cldsp.newInstance();
                break;
            } catch (InstantiationException | IllegalAccessException e) {
                throw new IOException("Class instance creation failed", e);
            }
        }
        if(dsp != null) {
            dsp.setContext(cxt);
            dsp.open(uri);
        }
        return dsp;
    }

    /**************************************************/
    // Exported versions for NetcdfFile and String

    static public synchronized DSP open(DapRequest drq, NetcdfFile ncfile, DapContext cxt)
            throws IOException
    {
        assert cxt != null && ncfile != null;
        // Convert to CDMDSP
        DSP dsp = open(ncfile,cxt);
        return dsp;
    }

    static public synchronized DSP open(DapRequest drq, String target, DapContext cxt)
            throws IOException
    {
        assert cxt != null && target != null;
        String path = null;
        DSP dsp = null;

        // See if this parses as a URL
        try {
            URI uri = new URI(target);
            // Windows drive letters cause URI to succeed, so special hack for that
            if (uri.getScheme().length() == 1 && driveletters.indexOf(uri.getScheme().charAt(0)) >= 0)
                throw new URISyntaxException("windows drive letter",target);
            // If uri protocol is file, then extract the path
            if(uri.getScheme().equals("file"))
                path = uri.getPath();
            else
                dsp = open(uri,cxt); // open as general URI
        } catch (URISyntaxException use) {
            // assume it is a simple file path
            path = target;
        }

        if(dsp == null) {
            // See if this can open as a NetcdfFile|NetcdfDataset
            NetcdfFile ncfile = null;
            try {
                ncfile = drq.getController().getNetcdfFile(drq,path);
            } catch (IOException ioe) {
                ncfile = null;
            }
            if(ncfile != null) {
                dsp = open(ncfile,cxt);
            }
        }

        if(dsp == null) {
            // Finally, try to open as a some kind of File object
            File file = new File(path);
            // Complain if it does not exist
            if(!file.exists())
                throw new DapException("Not found: " + target)
                        .setCode(DapCodes.SC_NOT_FOUND);
            else
                dsp = open(file,cxt);
            if(dsp != null) dsp.setContext(cxt);
            ((AbstractDSP)dsp).open(file);
        }
        return dsp;
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
