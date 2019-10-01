/* Copyright 2012, UCAR/Unidata.
   See the LICENSE file for more information.
*/

package dap4.core.data;

import dap4.core.dmr.DapDataset;
import dap4.core.dmr.DapVariable;
import dap4.core.util.DapContext;
import dap4.core.util.DapException;
import dap4.core.util.Slice;

import java.io.IOException;
import java.nio.ByteOrder;
import java.util.List;
import java.util.Map;

/*
The DAP4 code has support for data sources other than those that
can be represented as NetcdfFile objects. This is reflected
and wrapped by the DSP abstraction.  In particular, it is
possible to build a DSP that wraps a stream or file containing
direct DAP4 protocol encoded information, as opposed to CDM
structured information via NetcdfFile.

The current set of cases is as follows:

1. Data sources representable as NetcdfFile objects are used
   by CDMDSP on servers to access the object structured as
   CDM. The DSP then provides an API that the DAP4 code can then
   convert to the DAP4 protocol and send out to clients.

2. DAP4 encoded data sources are used by client programs to
   interpret a DAP4 protocol stream (or file if used for
   testing).  This stream is then made available on clients in
   the form of CDM.

So we have these cases:

1. NetcdfFile -> DSP (i.e. CDMDSP) -> DAP4 (on the server)
2. DAP4 -> DSP (i.e. HttpDSP) -> CDM (on the client)
3. DAP4 test file data -> DSP (i.e. FileDSP) -> DAP4 (on the server)
4. DAP4 test file data -> DSP (i.e. FileDSP) -> CDM (on the client)

The last two cases are specifically for testing, so cases 1 and 2 are
the critical ones.

The bottom line is that the DSP interface exposes no ability to
create a DSP, although some parametric setting is allowed.

The actual construction of a DSP instance is handled by DapCache.
*/

public interface DSP
{
    public DapDataset getDMR() throws dap4.core.util.DapException;

    public DataCursor getVariableData(DapVariable var) throws DapException;

    public ByteOrder getOrder();

    public ChecksumMode getChecksumMode();

    public void close() throws IOException;

    public Object getContext();
    public void setContext(DapContext cxt);

    public String getLocation();

}
