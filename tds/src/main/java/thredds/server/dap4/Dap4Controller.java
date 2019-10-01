/*
 * Copyright (c) 2012-2018 University Corporation for Atmospheric Research/Unidata
 * See LICENSE for license information.
 */

package thredds.server.dap4;

import dap4.core.util.DapContext;
import dap4.core.util.DapException;
import dap4.core.util.DapUtil;
import dap4.dap4lib.DapCodes;
import dap4.servlet.DapController;
import dap4.servlet.DapRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import thredds.core.DatasetManager;
import thredds.core.TdsRequestedDataset;
import ucar.nc2.NetcdfFile;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.*;

@Controller
@RequestMapping("/dap4")
public class Dap4Controller extends DapController
{

    //////////////////////////////////////////////////
    // Constants

    static final boolean DEBUG = false;

    static final boolean PARSEDEBUG = false;

    // NetcdfDataset enhancement to use: need only coord systems
    //static Set<NetcdfDataset.Enhance> ENHANCEMENT = EnumSet.of(NetcdfDataset.Enhance.CoordSystems);

    //////////////////////////////////////////////////
    // Type Decls

    //////////////////////////////////////////////////
    // Spring Elements

    @Autowired
    private ServletContext servletContext;

    @RequestMapping("**")
    public void handleRequest(HttpServletRequest req, HttpServletResponse res)
            throws IOException
    {
        super.handleRequest(req, res);
    }

    //////////////////////////////////////////////////
    // Constructor(s)

    public Dap4Controller()
    {
        super();
    }

    //////////////////////////////////////////////////////////

    @Override
    protected void
    doFavicon(String icopath, DapContext cxt)
            throws IOException
    {
        throw new UnsupportedOperationException("Favicon");
    }

    @Override
    protected void
    doCapabilities(DapRequest drq, DapContext cxt)
            throws IOException
    {
        addCommonHeaders(drq);
        OutputStream out = drq.getOutputStream();
        PrintWriter pw = new PrintWriter(new OutputStreamWriter(out, DapUtil.UTF8));
        pw.println("Capabilities page not supported");
        pw.flush();
    }

    @Override
    public long
    getBinaryWriteLimit()
    {
        return DEFAULTBINARYWRITELIMIT;
    }

    @Override
    public String
    getServletID()
    {
        return "dap4";
    }

    /*
     * For both getResourceRoot and getResourceFile:
     * There is a problem Spring under intellij when using mocking.
     * See TestServlet for more info.  In any case, if autowiring does
     * not work, then TdsRequestedDataset.getLocationFromRequestPath
     * will fail because it internal DatasetManager value will be null.
     * Autowiring would have set it to non-null. So, check to see if
     * the autowiring worked and if so use one of two different mechanisms.
     * This is completely a hack until such time as we can get things
     * to work under Intellij.
    */
    @Override
    public NetcdfFile
    getNetcdfFile(DapRequest drq, String location)
            throws DapException
    {
        NetcdfFile ncfile = null;
        String root = getResourceRoot(drq);
        String path = DapUtil.canonjoin(root, location);
        if(true && TdsRequestedDataset.getDatasetManager() == null) {
            TdsRequestedDataset.setDatasetManager(new DatasetManager());
        }
        if(TdsRequestedDataset.getDatasetManager() != null) {
            try {
                ncfile = TdsRequestedDataset.getNetcdfFile(drq.getRequest(), drq.getResponse(), path);
            } catch (IOException ioe) {
                ncfile = null;
            }
        } else {
            assert TdsRequestedDataset.getDatasetManager() == null;
            try {
                ncfile = NetcdfFile.open(path);
            } catch (IOException ied) {
                ncfile = null;
            }
        }
        if(ncfile == null)
            throw new DapException("Not found: " + location)
                    .setCode(DapCodes.SC_NOT_FOUND);
        if(!TESTING) {
            if(!TdsRequestedDataset.resourceControlOk(drq.getRequest(), drq.getResponse(),
                    ncfile.getLocation()))
                throw new DapException("Not authorized: " + location)
                        .setCode(DapCodes.SC_FORBIDDEN);
        }
        return ncfile;
    }

    @Override
    public String
    getResourceRoot(DapRequest drq)
            throws DapException
    {
        String rootpath;
        if(TdsRequestedDataset.getDatasetManager() != null) {
            rootpath = TdsRequestedDataset.getLocationFromRequestPath("");
        } else {
            assert TdsRequestedDataset.getDatasetManager() == null;
            rootpath = drq.getResourceRoot();
        }
        // Root path must exist
        File f = (rootpath == null ? null : new File(rootpath));
        if(f == null || !f.exists() || !f.canRead() || !f.isDirectory())
            throw new DapException("Resource root path not found")
                    .setCode(DapCodes.SC_NOT_FOUND);
        return rootpath;
    }
}


