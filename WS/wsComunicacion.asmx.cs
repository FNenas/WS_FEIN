using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Services;

namespace WS
{
    /// <summary>
    /// Summary description for wsComunicacion
    /// </summary>
    [WebService(Namespace = "http://tempuri.org/")]
    [WebServiceBinding(ConformsTo = WsiProfiles.BasicProfile1_1)]
    [System.ComponentModel.ToolboxItem(false)]
    // To allow this Web Service to be called from script, using ASP.NET AJAX, uncomment the following line. 
    [System.Web.Script.Services.ScriptService]


    public class wsComunicacion : System.Web.Services.WebService
    {

        string Pass = "N1e2n3a4s5";
        string sRuta = "C:/sXML/";
        string sArchivo = "";


        [WebMethod]
        public string HelloWorld2(string sNombres)
        {
            return "Hello World " + sNombres;
        }


        [WebMethod(Description = "Recibie texto de 1 a n y cuando acompleta guarda en xml , con el nombre id")]

        public Boolean RecibirInformacion(String sID, String sTexto, int nConsecutivo, int nTotal)
        {
            //sID  numero de identificacion "."consecutivo

            //alamcenar  todo en un array y cuando sea el ultimo procesar la informacion y quitarlo de la memoria
            try
            {

                if (nConsecutivo == nTotal)
                {
                    //leer  todos  los  archivos
                    sArchivo = "";
                    for (int x = 1; x <= nTotal; x++)
                    {
                        if (System.IO.File.Exists(sRuta + sID + "." + x))
                        {

                            sArchivo += System.IO.File.ReadAllText(sRuta + sID + "." + x);
                        }
                    }

                    sArchivo += sTexto;


                    //guardar en un archivo
                    System.IO.File.WriteAllText(sRuta + sID + ".xml", sArchivo);

                    for (int x = 1; x < nTotal; x++)
                    {
                        //eliminar archivos  locales
                        if (System.IO.File.Exists(sRuta + sID + "." + x))
                        {
                            System.IO.File.Delete(sRuta + sID + "." + x);
                        }

                    }


                }
                else
                {
                    //grabar el archivo   temporal 
                    System.IO.File.WriteAllText(sRuta + sID + "." + nConsecutivo, sTexto);


                }


                return true;


            }
            catch (Exception ex)
            {

                System.IO.File.WriteAllText(sRuta + sID + "_" + nConsecutivo + ".err", sArchivo + "\n" + ex.Message);
                //       Console.Write(ex.Message);
                return false;
            }

        }

        [WebMethod]
        public string obtenerRepartidores(int nSucursalID)
        {
            String sRepartidores = "";
            Boolean bEntro = false;
            System.Data.Odbc.OdbcConnection MyConnection = null;
            System.Data.Odbc.OdbcDataReader MyData = null;
            try
            {
                // Connection by using a DSN
                // MyODBCDatabase is an ODBC data source defined with odbcad32
                // OdbcConnection nwindConn = new OdbcConnection("DSN=MyODBCDatabase");
                // Connection by passing the connections parameters directly
                // ANA=<Full path of the analysis>
                // DIR=<Directory of the files>
                MyConnection = new System.Data.Odbc.OdbcConnection("DSN=HyperFileFruteria");
                // Open the connection
                MyConnection.Open();
                // Query that must be run on the database
                System.Data.Odbc.OdbcCommand MyQuery = new System.Data.Odbc.OdbcCommand("SELECT * from AppRepartidores  where AppRepartidores.Activo=1 and AppRepartidores.SucursalesID=" + nSucursalID, MyConnection);
                bEntro = true;
                // Run the query
                MyData = MyQuery.ExecuteReader();
                // Browse the result of the query
                string id;
                string Nombre = "";

                while (MyData.Read())
                {
                    if (sRepartidores.Length != 0)
                    {
                        sRepartidores += "|";
                    }
                    //   System.Diagnostics.EventLog.WriteEntry("wsComunicaciones", "Source = " + MyData.GetString(2), System.Diagnostics.EventLogEntryType.Error, 6805, 1);

                    id = MyData.GetInt64(0).ToString();
                    Nombre = MyData.GetString(2).ToString();
                    sRepartidores += id + "," + Nombre;


                    //   Console.WriteLine("\t{0}\t{1}", MyData.GetInt32(0), MyData.GetString(1));
                }
                //  MyData.Close();
                //     MyConnection.Close();
            }
            catch (System.Data.Odbc.OdbcException eExcpt)
            {
                /*  System.Diagnostics.EventLog ev = new System.Diagnostics.EventLog();

                  if (System.Diagnostics.EventLog.Exists("wsComunicaciones") == false) {

                      System.Diagnostics.EventLog.CreateEventSource("wsComunicaciones", "wsComunicacion");

                  }
                  ev.Source = "wsComunicaciones";


                ev.WriteEntry( "Source = " + eExcpt.Source + " Message = " + eExcpt.Message, System.Diagnostics.EventLogEntryType.Error,666);
                 */
                sRepartidores = "Error: Source = " + eExcpt.Source + " Message = " + eExcpt.Message;
                // Display the errors
                Console.WriteLine("Source = " + eExcpt.Source);
                Console.WriteLine("Message = " + eExcpt.Message);
            }
            finally
            {
                if (bEntro)
                {

                    MyData.Close();
                    MyConnection.Close();
                }

            }
            // pause before exiting
            //  Console.ReadLine();

            return sRepartidores;
        }
        [WebMethod]
        public Boolean registrarsQry(String sQry, String pPass)
        {
            //Se enviar qury separados por ;
            if (pPass != "N1e2n3a4s5")
            {
                return false;
            }
            System.Data.Odbc.OdbcConnection MyConnection = null;
            // ODBC command and transaction objects
            System.Data.Odbc.OdbcCommand command = new System.Data.Odbc.OdbcCommand();
            System.Data.Odbc.OdbcTransaction transaction = null;
            try
            {

                string[] numbers = sQry.Split(';');
                if (numbers.Length > 0)
                {

                    MyConnection = new System.Data.Odbc.OdbcConnection("DSN=HyperFileFruteria");
                    // Open the connection
                    MyConnection.Open();
                    // start the transaction
                    transaction = MyConnection.BeginTransaction();

                    // Assign transaction object for a pending local transaction.
                    command.Connection = MyConnection;
                    command.Transaction = transaction;
                    //  System.Data.Odbc.OdbcCommand MyQuery = null;


                    for (int i = 0; i < numbers.Length; i++)
                    {
                        //  try {
                        if (numbers[i].Length > 5)
                        {
                            // run the insert using a non query call
                            command.CommandText = numbers[i];
                            command.ExecuteNonQuery();

                            numbers[i] = "";
                        }

                        /*  } catch {
                              transaction.Rollback();

                          }*/

                    }
                    transaction.Commit();


                    MyConnection.Close();

                    return true;
                }
                else
                {

                    return false;
                }


            }
            catch (Exception ex)
            {
                transaction.Rollback();

                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", ex.Message);
                return false;
            }
            finally
            {
                try
                {
                    MyConnection.Close();
                }
                catch { }
            }
        }


        [WebMethod]

        public string executarQRY(String sQry, String NombreTable, String pPass)
        {
            if (pPass != "N1e2n3a4s5")
            {

                return "Error :Password in correcto";
            }

            Boolean bEntro = false;
            String sRespuesta = "";
            System.Data.Odbc.OdbcConnection objConn = null;
            System.Data.Odbc.OdbcDataAdapter dtAdapter = null;
            System.Data.DataTable dt = null;


            try
            {
                objConn = new System.Data.Odbc.OdbcConnection();
                dtAdapter = new System.Data.Odbc.OdbcDataAdapter();
                dt = new System.Data.DataTable();

                String strConnString;

                strConnString = "DSN=HyperFileFruteria";
                objConn = new System.Data.Odbc.OdbcConnection(strConnString);
                objConn.Open();

                dtAdapter = new System.Data.Odbc.OdbcDataAdapter(sQry, objConn);

                dtAdapter.Fill(dt);
                dt.TableName = NombreTable;

                System.Xml.XmlElement xmlElement = Serialize(dt);
                //  Console.WriteLine(xmlElement.ToString());

                sRespuesta = xmlElement.OuterXml.ToString();

            }
            catch (System.Data.Odbc.OdbcException eExcpt)
            {

                sRespuesta = "Error: Source = " + eExcpt.Source + " Message = " + eExcpt.Message;
                // Display the errors
                Console.WriteLine("Source = " + eExcpt.Source);
                Console.WriteLine("Message = " + eExcpt.Message);
            }
            finally
            {
                if (bEntro)
                {

                    dtAdapter = null;
                    objConn.Close();
                    objConn = null;
                }

            }
            //  le.ReadLine();

            return sRespuesta;
        }

        private System.Xml.XmlElement Serialize(object obj)
        {
            //   System.Xml.XmlElement serializedXmlElement = new System.Xml.XmlElement();
            System.Xml.XmlDocument xmlDocument = new System.Xml.XmlDocument();
            try
            {
                System.IO.MemoryStream memoryStream = new System.IO.MemoryStream();
                System.Xml.Serialization.XmlSerializer xmlSerializer = new System.Xml.Serialization.XmlSerializer(obj.GetType());
                xmlSerializer.Serialize(memoryStream, obj);
                memoryStream.Position = 0;


                xmlDocument.Load(memoryStream);
                //  serializedXmlElement = xmlDocument.DocumentElement;
            }
            catch (Exception e)
            {
                //logging statements. You must log exception for review

            }

            return xmlDocument.DocumentElement;
        }

        private string obtenerKey(string pKey)
        {

            string[] numbers = pKey.Split('.');
            return numbers[0];
        }

        [WebMethod]
        public String existenciaLocal(String ArticuloID, String SucursalesID)
        {

            Decimal existencia = 0;
            String sQry = "SELECT ArticulosExistencias.Existencia AS Existencia FROM ArticulosExistencias	WHERE ArticulosExistencias.ArticulosID= " + ArticuloID + " AND	ArticulosExistencias.SucursalesID =" + SucursalesID;

            Decimal xCantidadVendidaPadre = 0;
            Decimal rCantidadReciduoHijos = 0;
            int nCantidadSalida = 0;
            Decimal nCantidadBomPorAfectar = 0;
            Decimal aux = 0;
            Decimal pCantidadBOM = 0;
            Decimal pAcumulado = 0;
            Decimal pResiduo = 0;
            int nArticuloID = 0;

            System.Data.Odbc.OdbcConnection MyConnection = new System.Data.Odbc.OdbcConnection("DSN=HyperFileFruteria");
            System.Data.DataSet ds = new System.Data.DataSet();
            System.Data.DataSet ds1 = new System.Data.DataSet();
            System.Data.Odbc.OdbcDataAdapter da = null;
            System.Data.Odbc.OdbcCommand command = null;
            try
            {


                //existencia
                command = new System.Data.Odbc.OdbcCommand(sQry, MyConnection);
                da = new System.Data.Odbc.OdbcDataAdapter(command);
                da.Fill(ds);
                existencia = decimal.Parse(ds.Tables[0].Rows[0][0].ToString());
                ds = null;
                da = null;
                //contabilizar los bom por afectar

                ds = new System.Data.DataSet();

                sQry = "SELECT BOM_Articulos.BOM_ArticulosID AS BOM_ArticulosID,BOM_Articulos.ArticulosID AS ArticulosID,BOM_Articulos.ArticuloBOMID AS ArticuloBOMID,	BOM_Articulos.Acumulador AS Acumulador,	BOM_Articulos.Residuo AS Residuo, BOM_Articulos.SucursalesID AS SucursalesID,Articulos.CantidadBOM AS CantidadBOM FROM Articulos,	BOM_Articulos WHERE  Articulos.ArticulosID = BOM_Articulos.ArticuloBOMID   AND  (   BOM_Articulos.SucursalesID =" + SucursalesID + "  AND BOM_Articulos.ArticuloBOMID = " + ArticuloID + ")";
                command.CommandText = sQry;
                da = new System.Data.Odbc.OdbcDataAdapter(command);
                da.Fill(ds);

                foreach (System.Data.DataRow r in ds.Tables[0].Rows)
                {
                    da = null;
                    try { nArticuloID = Convert.ToInt32(r[1].ToString()); } catch { nArticuloID = -1; }


                    sQry = " SELECT  SUM(Ventas_Articulos.Cantidad) AS sum_Cantidad FROM Ventas, Ventas_Articulos,  CortesY WHERE  Ventas.VentasID = Ventas_Articulos.VentasID  AND CortesY.CortesYID = Ventas.CortesYID  AND (   Ventas_Articulos.ArticulosID = " + nArticuloID + "  AND CortesY.AfectoKardex = 0  AND Ventas.SucursalesID = " + SucursalesID + " )";
                    command.CommandText = sQry;
                    da = new System.Data.Odbc.OdbcDataAdapter(command);
                    da.Fill(ds1);
                    xCantidadVendidaPadre = 0;
                    try
                    {
                        xCantidadVendidaPadre = decimal.Parse(ds1.Tables[0].Rows[0][0].ToString());
                    }
                    catch
                    {
                        //xCantidadVendidaPadre = 0;
                    }
                    rCantidadReciduoHijos = 0;
                    try { pCantidadBOM = decimal.Parse(r[6].ToString()); } catch { pCantidadBOM = 0; }

                    try { pAcumulado = decimal.Parse(r[3].ToString().Replace('"', ' ')); } catch { pAcumulado = 0; }
                    try { pResiduo = decimal.Parse(r[4].ToString()); } catch { pResiduo = 0; }





                    //QRY_BomArticulos_X_ArticuloHijoID.CantidadBOM  QRY_BomArticulos_X_ArticuloHijoID.Acumulador
                    if (pCantidadBOM != 0 && pAcumulado != -1)
                    {
                        rCantidadReciduoHijos = 0;

                        rCantidadReciduoHijos = (decimal.Parse(xCantidadVendidaPadre.ToString()) * pAcumulado);

                        rCantidadReciduoHijos += pResiduo;// QRY_BomArticulos_X_ArticuloHijoID.Residuo

                        //                        falta
                        if (pCantidadBOM > 0)
                        {
                            aux = rCantidadReciduoHijos / pCantidadBOM;

                            nCantidadSalida = Convert.ToInt32(aux);


                        }
                        else
                        {
                            nCantidadSalida = 0;
                        }
                        nCantidadBomPorAfectar += nCantidadSalida;

                    }
                    else
                    {
                        if (Decimal.Parse(r[3].ToString()) == -1)
                        {
                            nCantidadBomPorAfectar += xCantidadVendidaPadre;

                        }

                    }



                }


                existencia = existencia - nCantidadBomPorAfectar;
                MyConnection.Close();



            }
            catch (Exception ex)
            {


                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err","ExistenciaLocal:"+ ex.Message+ex.StackTrace);
                existencia = 0;
            }
            finally
            {
                try
                {

                    ds = null;
                    ds1 = null;
                    da = null;
                    command = null;
                    MyConnection.Close();
                }
                catch { }
            }



            return existencia.ToString();
        }

        private System.Data.DataSet qryToDataSet(String qry)
        {

            System.Data.Odbc.OdbcConnection MyConnection = new System.Data.Odbc.OdbcConnection("DSN=HyperFileFruteria");
            System.Data.DataSet ds = new System.Data.DataSet();

            System.Data.Odbc.OdbcDataAdapter da = null;
            System.Data.Odbc.OdbcCommand command = null;


            try
            {

                //existencia
                command = new System.Data.Odbc.OdbcCommand(qry, MyConnection);
                da = new System.Data.Odbc.OdbcDataAdapter(command);
                da.Fill(ds);


                MyConnection.Close();



            }
            catch (Exception ex)
            {


                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "qryToDataSet:"+ ex.Message+"\n"+ex.StackTrace);
                ds = null;
            }
            finally
            {
                try
                {

                    da = null;
                    command = null;
                    MyConnection.Close();
                }
                catch { }
            }



            return ds;
        }
        private Boolean qryInsertUpdate(String qry)
        {

            System.Data.Odbc.OdbcConnection MyConnection = new System.Data.Odbc.OdbcConnection("DSN=HyperFileFruteria");


            System.Data.Odbc.OdbcTransaction transaction = null;
            System.Data.Odbc.OdbcCommand command = null;
            MyConnection.Open();
            transaction = MyConnection.BeginTransaction();
            try
            {
                //existencia
                command = new System.Data.Odbc.OdbcCommand(qry, MyConnection);
                command.Transaction = transaction;
                command.ExecuteNonQuery();
                transaction.Commit();

                MyConnection.Close();


                return true;
            }
            catch (Exception ex)
            {
                transaction.Rollback();

                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "qryInsertUpdate:"+ ex.Message+ex.StackTrace);

            }
            finally
            {
                try
                {

                    command = null;
                    MyConnection.Close();
                }
                catch { }
            }



            return false;
        }
        [WebMethod(Description = " Si existe el registro regrresa un valor int SucursalID, si no regresa un -1")]
        public int almacenSurteArticulo(String ArticulosID, String SucursalesID)
        {

            int Id = 0;

            String sQuery = "SELECT * FROM [AlmacenesArticulos] WHERE (SucursalBaseID=" + SucursalesID + ") AND(ArticulosID=" + ArticulosID + ") ";
            System.Data.DataSet ds = qryToDataSet(sQuery);

            if (ds == null)
            {
                // no hay registro
                Id = -1;
            }
            else
            {
                try
                {
                    Id = Convert.ToInt32(ds.Tables[0].Rows[0]["SucursalAlmacenID"]);
                }
                catch
                {

                    Id = -1;
                }
            }




            return Id;

        }
        [WebMethod(Description = "Regresa xml")]
        public String precioArticuloRegistro(String ClienteID, String ArticulosID, String SucursalesID)
        {
            //sucursal id 

            String query = "SELECT ArticulosPrecioMayoreo.* from ArticulosPrecioMayoreo,Clientes,PorcentajeMayoreo" +
       " where " +
      "  PorcentajeMayoreo.Valor = Clientes.PorcentajeDescuento " +
       " and ArticulosPrecioMayoreo.PorcentajeMayoreoID = PorcentajeMayoreo.PorcentajeMayoreoID " +
       " and Clientes.ClientesID = " + ClienteID +
       " and ArticulosPrecioMayoreo.ArticulosID =" + ArticulosID +
       " and ArticulosPrecioMayoreo.Activo = 1";


            // se regresara un string separado por pipes|  si existe in ~ solto de linea
            //    valor|valor|valor|valor|~|valor|valor|valoer

            System.Data.DataSet ds = qryToDataSet(query);





            System.Xml.XmlElement xmlElement = Serialize(ds.Tables[0]);

            return xmlElement.OuterXml.ToString();
            //   return sRespuesta;


        }
        [WebMethod(Description = "Regresa xml")]
        public String obtenerPreciosCliente(String ClienteID, String sSucursalesID)
        {
            //sucursal id 
            String query = "";
            if (ClienteID == "")
            {
                query = "SELECT ArticulosPrecioMayoreo.*,PorcentajeMayoreo.Valor as ValorPorcentaje,Articulos.Nombre,Articulos.Codigo,Lineas.NombreLinea as LineaNombre,Lineas.LineasID as LineasID,ArticulosExistencias.Existencia from ArticulosPrecioMayoreo,PorcentajeMayoreo,Articulos,lineas,ArticulosExistencias " +
     " where " +

     " Articulos.ArticulosID=ArticulosPrecioMayoreo.ArticulosID and Articulos.LineaID=Lineas.LineasID AND " +
        "  Articulos.ArticulosID=ArticulosExistencias.ArticulosID  " +
       " and ArticulosPrecioMayoreo.PorcentajeMayoreoID = PorcentajeMayoreo.PorcentajeMayoreoID " +
       " and ArticulosExistencias.SucursalesID =" + sSucursalesID +
     " and ArticulosPrecioMayoreo.Activo = 1  and PorcentajeMayoreo.Activo=1";

            }
            else
            {
                query = "SELECT ArticulosPrecioMayoreo.*,Articulos.Nombre,Articulos.Codigo,Lineas.NombreLinea as LineaNombre,Lineas.LineasID as LineasID,ArticulosExistencias.Existencia from ArticulosPrecioMayoreo,Clientes,PorcentajeMayoreo,Articulos,lineas,ArticulosExistencias " +
     " where " +

     " Articulos.ArticulosID=ArticulosPrecioMayoreo.ArticulosID and Articulos.LineaID=Lineas.LineasID AND " +
        "  Articulos.ArticulosID=ArticulosExistencias.ArticulosID  " +
    " AND PorcentajeMayoreo.Valor = Clientes.MargenUtilidadMayoreo " +
     " and ArticulosPrecioMayoreo.PorcentajeMayoreoID = PorcentajeMayoreo.PorcentajeMayoreoID " +
     " and Clientes.ClientesID = " + ClienteID +
     " and ArticulosExistencias.SucursalesID =" + sSucursalesID +
     " and ArticulosPrecioMayoreo.Activo = 1 and PorcentajeMayoreo.Activo=1";


            }



            System.Data.DataSet ds = qryToDataSet(query);



            System.Xml.XmlElement xmlElement = Serialize(ds.Tables[0]);

            return xmlElement.OuterXml.ToString();
            //   return sRespuesta;


        }
        [WebMethod(Description = "Regresa xml")]
        public String obtenerPreciosPorcentaje(String PorcentarID, String sSucursalesID)
        {
            //sucursal id 
            String query = "";
          
                query = "SELECT ArticulosPrecioMayoreo.*,PorcentajeMayoreo.Valor as ValorPorcentaje,Articulos.Nombre,Articulos.Codigo,Lineas.NombreLinea as LineaNombre,Lineas.LineasID as LineasID,ArticulosExistencias.Existencia from ArticulosPrecioMayoreo,PorcentajeMayoreo,Articulos,lineas,ArticulosExistencias " +
     " where " +

     " Articulos.ArticulosID=ArticulosPrecioMayoreo.ArticulosID and Articulos.LineaID=Lineas.LineasID AND " +
        "  Articulos.ArticulosID=ArticulosExistencias.ArticulosID  " +
       " and ArticulosPrecioMayoreo.PorcentajeMayoreoID = PorcentajeMayoreo.PorcentajeMayoreoID " +
       " and ArticulosExistencias.SucursalesID =" + sSucursalesID +
     " and ArticulosPrecioMayoreo.Activo = 1  and PorcentajeMayoreo.Activo=1 and ArticulosPrecioMayoreo.PorcentajeMayoreoID ="+PorcentarID;

        
        



            System.Data.DataSet ds = qryToDataSet(query);



            System.Xml.XmlElement xmlElement = Serialize(ds.Tables[0]);

            return xmlElement.OuterXml.ToString();
            //   return sRespuesta;


        }
        [WebMethod(Description = "Actualiza posicion de cliente")]
        public Boolean actualizarPosicionCliente(int nClienteID, String nLat, String nLog)
        {

            String query = "Update [Clientes] set Latitud=" + nLat + ",Longitud=" + nLog + " WHERE ClientesID=" + nClienteID;


            return qryInsertUpdate(query);
        }
        [WebMethod(Description = "Regresa un xml")]
        public String obtenerClientesCercas(Boolean bCercas, String lat, String log)
        {

            String sResultado = "";

            String query = "SELECT * FROM [Clientes] WHERE (Activo='1') and esMayoreoAprobado=1 and esMayoreo=1 ";


            

            System.Data.DataSet ds = qryToDataSet(query);


            System.Xml.XmlElement xmlElement = Serialize(ds.Tables[0]);

            return xmlElement.OuterXml.ToString();
        }
        [WebMethod(Description = "Regresa un xml del cliente enviado")]
        public String obtenerClientes(String sClientesID)
        {

            //    String sResultado = "";

            String query = "SELECT * FROM [Clientes] WHERE ClientesID=" + sClientesID;



            System.Data.DataSet ds = qryToDataSet(query);


            System.Xml.XmlElement xmlElement = Serialize(ds.Tables[0]);

            return xmlElement.OuterXml.ToString();
        }
        [WebMethod(Description = "Regresa un xml del cliente enviado")]
        public String obtenerPedidos(String sClientesID,String sMes)
        {

            //    String sResultado = "";

            String query = "SELECT *,(SELECT count (PreVenta_Mayoreo_Detalle.ArticulosID) AS No from PreVenta_Mayoreo_Detalle where PreVenta_Mayoreo_Detalle.PreVenta_MayoreoID=PreVenta_Mayoreo.PreVenta_MayoreoID) as NoProductos FROM [PreVenta_Mayoreo] WHERE (Activo='1') and ClientesID=" + sClientesID + " and FechaHora BETWEEN '"+sMes+"01000001' and '"+sMes+"31235959' order by FechaHora";



            System.Data.DataSet ds = qryToDataSet(query);


            System.Xml.XmlElement xmlElement = Serialize(ds.Tables[0]);

            return xmlElement.OuterXml.ToString();
        }
        [WebMethod(Description = "Regresa un xml del cliente enviado")]
        public String obtenerPedidosDetalle(String sPedidosID)
        {

            //    String sResultado = "";

            String query = "SELECT PreVenta_Mayoreo_Detalle.ArticulosID,Articulos.Nombre as NombreArticulo, Articulos.Codigo,PreVenta_Mayoreo_Detalle.PrecioUCIVA_Cliente as PrecioCIVA,PreVenta_Mayoreo_Detalle.Cantidad,PreVenta_Mayoreo_Detalle.Importe,PreVenta_Mayoreo_Detalle.Notas " +
"  from PreVenta_Mayoreo_Detalle, Articulos, PreVenta_Mayoreo " +
" where PreVenta_Mayoreo_Detalle.ArticulosID = Articulos.ArticulosID " +
" and PreVenta_Mayoreo_Detalle.PreVenta_MayoreoID = PreVenta_Mayoreo.PreVenta_MayoreoID " +
" and PreVenta_Mayoreo_Detalle.PreVenta_MayoreoID =" + sPedidosID +
" order by PreVenta_Mayoreo.FechaHora Desc";



            System.Data.DataSet ds = qryToDataSet(query);


            System.Xml.XmlElement xmlElement = Serialize(ds.Tables[0]);

            return xmlElement.OuterXml.ToString();
        }

        [WebMethod(Description = "registra pedido FECHAHORA|CLIENTEID|PORCENTAJEMAYOREID|NOMBRECLIENTE|SUBTOTAL|IVA|IMPORTE|EMPLEADOID|NOTAS|FECHAENTREGA|Facturacion|TipoCambio|importedlls")]
        public int HaddPedido(String sPedido,String sDetalle)
        {
            Boolean bBandera = false;
            String[] arr;
            int ID = -1;
            try
            {
               
                arr = sPedido.Split('|');


                //  String qry = "Insert into PreVenta_Mayoreo (FechaHora,ClientesID,PorcentajeMayoreoID,NombreCliente,Subtotal,IVA,Importe,EmpleadosID,Notas,FechaEntrega,Activo) " +
                //                         " Values ('" + arr[0] + "'," + arr[1] + "," + arr[2] + ",'" + arr[3] + "'," + arr[4] + "," + arr[5] + "," + arr[6] + "," + arr[7] + ",'" + arr[8] + "','" + arr[9] + "',1)";
                String qry = "";

                // version 2  para  facturacion
                if (arr.Length > 11) {
                    qry = "Insert into PreVenta_Mayoreo (FechaHora,ClientesID,PorcentajeMayoreoID,NombreCliente,Subtotal,IVA,Importe,EmpleadosID,Notas,FechaEntrega,Activo,Facturacion,Tipo_Cambio,ImporteDolares) " +
                                                   " Values ('" + arr[0] + "'," + arr[1] + "," + arr[2] + ",'" + arr[3] + "'," + arr[4] + "," + arr[5] + "," + arr[6] + "," + arr[7] + ",'" + arr[8] + "','" + arr[9] + "',1," + arr[10] + "," + arr[11] + "," + arr[12] + ")";

                }
                else {
                    qry = "Insert into PreVenta_Mayoreo (FechaHora,ClientesID,PorcentajeMayoreoID,NombreCliente,Subtotal,IVA,Importe,EmpleadosID,Notas,FechaEntrega,Activo,Facturacion) " +
                                                   " Values ('" + arr[0] + "'," + arr[1] + "," + arr[2] + ",'" + arr[3] + "'," + arr[4] + "," + arr[5] + "," + arr[6] + "," + arr[7] + ",'" + arr[8] + "','" + arr[9] + "',1," + arr[10] + ")";


                }


             
                //detectar ele de parametros



       

                bBandera = qryInsertUpdate(qry);

                if (bBandera)
                {
                    System.Data.DataSet ds = qryToDataSet("Select Max(Preventa_MayoreoID) from Preventa_Mayoreo");

                    if (ds.Tables.Count > 0)
                    {


                        ID = int.Parse(ds.Tables[0].Rows[0][0].ToString());
                    }

                }



               
            }
            catch (Exception ex)
            {


                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", ex.Message);
              
                ID = -1;
            }

            return ID;

        }

        [WebMethod(Description = "registra detalle pedido  // ArticulosID|Cantidad|IVA|Importe|PrecioUCIVA_Cliente|PrecioUSIVA_Cliente|CostoCIVA|CostoSIVA|PrecioCIVA|PrecioSIVA|Notas"  +" @ seperaer un detale de otro")]
       
        public String HaddPedidoDetalle(String sPedidoDetalle,String PreVenta_MayoreoID)
        {
         //   Boolean bBandera = false;
          
            String[] arrDetalle;

         String   sRespuesta = "";
            String qry="";

            String preciosIVa_Original = "0";
            String precioCIVA_Original = "0";
            String precioSiVA = "0";
            String precioCIVA = "0";
            try
            {

                arrDetalle = sPedidoDetalle.Split('@');


                foreach (String s in arrDetalle){
                    if (s.Length > 3)
                    {

                        String[] arr;
                        arr = s.Split('|');




                        if (arr[8] == "") {
                            precioCIVA_Original = "0";
                        }
                        else {
                            precioCIVA_Original = arr[8];
                        }

                        if (arr[9] == "")
                        {
                            preciosIVa_Original = "0";
                        }
                        else
                        {
                            preciosIVa_Original = arr[9];
                        }


                        if (arr[4] == "")
                        {
                            precioCIVA = "0";
                        }
                        else
                        {
                            precioCIVA = arr[4];
                        }

                        if (arr[5] == "")
                        {
                            precioSiVA = "0";
                        }
                        else
                        {
                            precioSiVA = arr[5];
                        }





                        //  0           1    2   3       4    5                       6                7          8         9         10
                        qry = "Insert into PreVenta_Mayoreo_Detalle (ArticulosID,Cantidad,IVA,Importe,PrecioUCIVA_CLiente,PrecioUSIVA_Cliente,CostoCIVA,CostoSIVA,PrecioCIVA,PrecioSIVA,PreVenta_MayoreoID,Notas) " +
                                                           " Values (" + arr[0] + "," + arr[1] + "," + arr[2] + "," + arr[3] + "," + precioCIVA + "," + precioSiVA + "," + arr[6] + "," + arr[7] + "," + precioCIVA_Original + "," + preciosIVa_Original + "," + PreVenta_MayoreoID + ",'"+arr[10]+"')";

                        // Buscar el ultimo id Agregado
                     //  System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + "_HADDDETALLE_.err", qry);

                        sRespuesta += qryInsertUpdate(qry);

                        sRespuesta += ",";
                        arr = null;
                    }
                }



            }
            catch (Exception ex)
            {


                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "PreVenta_MayoreoID:"+ PreVenta_MayoreoID +"/n" +ex.Message);

             
            }

            return sRespuesta;

        }

        [WebMethod(Description = "Regresa un xml del los porcentajes")]
        public String obtenerPorcentajesMAyoreo()
        {

            //    String sResultado = "";

            String query = "SELECT * FROM [PorcentajeMayoreo] WHERE (Activo='1')  ";



            System.Data.DataSet ds = qryToDataSet(query);


            System.Xml.XmlElement xmlElement = Serialize(ds.Tables[0]);

            return xmlElement.OuterXml.ToString();
        }
        public System.Drawing. Image byteArrayToImage(byte[] byteArrayIn)
        {
            System.Drawing.Image returnImage=null;


            try
            {
                System.IO.MemoryStream ms = new System.IO.MemoryStream(byteArrayIn, 0, byteArrayIn.Length);
                ms.Write(byteArrayIn, 0, byteArrayIn.Length);
                returnImage = System.Drawing.Image.FromStream(ms, true);//Exception occurs here
            }
            catch(Exception ex) {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "byteArraytoImage:/n" + ex.Message+ex.StackTrace);

            }
            return returnImage;
        }
        [WebMethod]
        public Boolean guardarFotoCliente(byte[] f, String sClienteID) {

            //no se utiliza
            // revisar  marca  error  10/12/2020
            System.Data.Odbc.OdbcTransaction transaction = null;
            System.Data.Odbc.OdbcCommand command = null;
            System.Data.Odbc.OdbcConnection MyConnection = null;
            Boolean bRespuesta = false;
           
            try
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "Imagen ClienteID:" + f.ToString());
                System.Drawing.Image newImage = byteArrayToImage(f);
                //   System.IO.MemoryStream ms = new System.IO.MemoryStream(f);
                newImage.Save(@"C:\sXML\tesd.jpg");



                MyConnection = new System.Data.Odbc.OdbcConnection("DSN=HyperFileFruteria");
            MyConnection.Open();
            transaction = MyConnection.BeginTransaction();
           
                String qry= "Update clientes set fotoLocal=? where ClientesID = " + sClienteID;



                command = new System.Data.Odbc.OdbcCommand(qry, MyConnection);
                System.Data.Odbc.OdbcParameter paramFileField = new System.Data.Odbc.OdbcParameter();

                paramFileField.OdbcType = System.Data.Odbc.OdbcType.Image;

                paramFileField.Value = newImage;

                command.Parameters.Add(paramFileField);


              

                command.Transaction = transaction;
                command.ExecuteNonQuery();
                transaction.Commit();
                bRespuesta = true;

                }
            catch (Exception ex)
            {

                bRespuesta = false;
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "Imagen ClienteID:" + sClienteID + "/n" + ex.Message+ex.StackTrace);

                transaction.Rollback();
            }
            finally {

                MyConnection.Close();
            }
          //  bRespuesta = qryInsertUpdate("Update clientes set fotoLocal="+img+" where ClientesID="+sClienteID);




            return bRespuesta;

        }

        [WebMethod(Description = "Genera pdf")]

        public Boolean PdfPedido(String PreVenta_MayoreoID)
        {
         
            try
            {

                String nombre = "C:/sXML/PED_" + PreVenta_MayoreoID;

                if (System.IO.File.Exists(nombre + ".pdf"))
                {

                    return true;
                }


                // byte[]
                Stimulsoft.Report.StiReport reporte = new Stimulsoft.Report.StiReport();
                reporte.Load("C:/sXML/Pedido.mrt");
                       String q = "SELECT PreVenta_Mayoreo.*,PreVenta_Mayoreo_Detalle.ArticulosID,Articulos.Nombre as NombreArticulo, Articulos.Codigo,PreVenta_Mayoreo_Detalle.PrecioUCIVA_Cliente as PrecioCIVA,PreVenta_Mayoreo_Detalle.Cantidad,PreVenta_Mayoreo_Detalle.Importe,PreVenta_Mayoreo_Detalle.Notas " +
           ",Clientes.RFC,Clientes.Direccion,Clientes.CorreoElectronico,Clientes.CorreoAlterno,(SELECT Empleados.NombreCompleto from Empleados where Empleados.EmpleadosID=PreVenta_Mayoreo.EmpleadosID) as Vendedor,(SELECT Empleados.NombreCompleto from Empleados where Empleados.EmpleadosID=PreVenta_Mayoreo.EmpleadoID_Autorizo) as Autorizo  from PreVenta_Mayoreo_Detalle, Articulos, PreVenta_Mayoreo,Clientes " +
           " where PreVenta_Mayoreo_Detalle.ArticulosID = Articulos.ArticulosID " +
           " and PreVenta_Mayoreo_Detalle.PreVenta_MayoreoID = PreVenta_Mayoreo.PreVenta_MayoreoID " +
           " and PreVenta_Mayoreo.ClientesID =Clientes.ClientesID " +
           " and PreVenta_Mayoreo_Detalle.PreVenta_MayoreoID =" + PreVenta_MayoreoID +
           " order by PreVenta_Mayoreo.FechaHora Desc";
                   System.Data.DataSet ds = qryToDataSet(q);
                ds.DataSetName = "MayoreoDetalle";
                ds.Tables[0].TableName= "MayoreoDetalle";

                ds.WriteXml(nombre + ".xml");


                reporte.RegData("MayoreoDetalle", "MayoreoDetalle",ds);
              //  reporte.Dictionary.Synchronize();
              

                reporte["id"] = PreVenta_MayoreoID;
               


                reporte.Compile();
                
                reporte.Render();

                reporte.ExportDocument(Stimulsoft.Report.StiExportFormat.Pdf, nombre+".pdf");

              
              return true;


            }
            catch (Exception ex)
            {

              
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "PdfPedido:"+ ex.Message+ "\n"+ex.StackTrace);

            }

            //   return    byte[] todecode_byte = Convert.FromBase64String(data); // data es el string en BASE64
            return false;
        }
        [WebMethod(Description = "get pdf")]
        public String GetPedidoPDF(String NoPedido) {
            try
            {

                byte[] bytes = System.IO.File.ReadAllBytes(@"C:\sXML\PED_"+ NoPedido+".pdf");
            return Convert.ToBase64String(bytes);
        }
            catch (Exception ex)
            {



                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", ex.Message);

            }
            return "";
}
        [WebMethod(Description = "EnviarPor corroe pdf")]
        public Boolean EnviarPedidoPDF(String NoPedido,String sDestino)
        {

            String correode = "info@fruteriasnenas.com";
            String clavecorreo = "N1e2n3a4s";

            // Replace recipient@example.com with a "To" address. If your account 
            // is still in the sandbox, this address must be verified.
            String TO = sDestino;

          
            // The subject line of the email
            String SUBJECT =
                "Envio de Preventa :"+NoPedido;

            // The body of the email
            String BODY =
                "<h1>Su Preventa se ha registrado Correctamente</h1>" +
                "<p>Se Anexa su comprobante de Preventa.</p>";

            // Create and build a new MailMessage object
            System.Net.Mail. MailMessage message = new System.Net.Mail. MailMessage();
            message.IsBodyHtml = true;
            message.From = new System.Net.Mail. MailAddress(correode, "Fruteria Nenas");
            message.To.Add(new System.Net.Mail. MailAddress(TO));
            message.Subject = SUBJECT;
            message.Attachments.Add(new System.Net.Mail.Attachment(@"C:\sXML\PED_" + NoPedido + ".pdf"));
            message.Body = BODY;
            // Comment or delete the next line if you are not using a configuration set
           // message.Headers.Add("X-SES-CONFIGURATION-SET", CONFIGSET);

            using (System.Net.Mail.SmtpClient smtp = new System.Net.Mail.SmtpClient())
            {
                try { 
                smtp.Host = "smtp.ipage.com";
                smtp.Port = 587;
                smtp.EnableSsl = false;
                System.Net.NetworkCredential NetworkCred = new System.Net.NetworkCredential();
                NetworkCred.UserName =correode;
                NetworkCred.Password = clavecorreo;
                smtp.UseDefaultCredentials = true;
                smtp.Credentials = NetworkCred;
                smtp.Send(message);


                return true;


                }
                catch (Exception ex)
                {
                    System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", ex.Message);

                }
            }
            return false;
        }

        [WebMethod(Description = "validacion de Usuario mobile, regresa el id y empleado")]
        public String validarLoguin(String sUsuario, String sPassword) {

            try { 
            String q = "SELECT Usuarios.UsuariosID,Empleados.EmpleadosID , Empleados.Codigo, Empleados.NombreCompleto from Empleados,Usuarios where Empleados.EmpleadosID=Usuarios.EmpleadosID and Usuarios.ConreasenaAPP='"+sPassword+"' and Usuarios.Nombre='"+sUsuario+"' and Usuarios.Activo=1 and empleados.Baja_temporal=0 and Empleados.Activo=1 ";


            System.Data.DataSet ds = qryToDataSet(q);


                if (ds.Tables.Count > 0) {


                    System.Xml.XmlElement xmlElement = Serialize(ds.Tables[0]);

                    return xmlElement.OuterXml.ToString();

                }

                    }
                catch (Exception ex)
                {
                    System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "validarLoguin:"+ ex.Message+ex.StackTrace);

                }

            return "";
        }
        [WebMethod(Description = "Actualiar contraseña")]
        public Boolean actualiarPassword(String nUsuarioID,String sNewPassword)
        {

            try
            {
                String q = "Update Usuarios set Usuarios.ConreasenaAPP='"+sNewPassword+"' Usuarios.UsuariosID=" + nUsuarioID+ "  ";

                return qryInsertUpdate(q);
              

            }
            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", ex.Message);

            }

            return false;
        }

        [WebMethod(Description = "Regresa un xml con las existencias configuradas")]
        public String obtenerExistenciaMayoreo() {

            try
            {
                String q = "SELECT * from  ExisteciaMayoreo ";


                System.Data.DataSet ds = qryToDataSet(q);


                if (ds.Tables.Count > 0)
                {


                    System.Xml.XmlElement xmlElement = Serialize(ds.Tables[0]);

                    return xmlElement.OuterXml.ToString();

                }

            }
            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "obtenerExistenciaMayoreo:"+ ex.Message+ex.StackTrace);

            }

            return "";
        }
        [WebMethod(Description = "Regresa un xml la informacion de pedidos")]
        public String obtenerPedidoAlmacen(String sPedidoID)
        {

            try
            {
                String q = "SELECT * from pedidos where pedidosID="+sPedidoID;


                System.Data.DataSet ds = qryToDataSet(q);


                if (ds.Tables.Count > 0)
                {


                    System.Xml.XmlElement xmlElement = Serialize(ds.Tables[0]);

                    return xmlElement.OuterXml.ToString();

                }

            }
            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "obtenerPedidoAlmacen:"+ ex.Message+ex.StackTrace);

            }

            return "";
        }

        [WebMethod(Description = "Regresa un xml la informacion de pedidos")]
        public String obtenerDetallePedidoAlmacen(String sPedidoID)
        {

            try
            {
                String q = "SELECT * from Pedidos_Articulos where PedidosID=" + sPedidoID;


                System.Data.DataSet ds = qryToDataSet(q);


                if (ds.Tables.Count > 0)
                {


                    System.Xml.XmlElement xmlElement = Serialize(ds.Tables[0]);

                    return xmlElement.OuterXml.ToString();

                }

            }
            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "obtenerDetallePedidoAlmacen:"+ ex.Message+ex.StackTrace);

            }

            return "";
        }

        [WebMethod(Description = "Regresa un xml la informacion de NOpedidos")]
        public String obtenerInfoEstatisticas(String sEmpleadoID,String sMes)
        {

            try
            {
                String q = "SELECT 'NoPreventas' as Tipo,count(EmpleadosID) as Cantidad from PreVenta_Mayoreo where EmpleadosID=" + sEmpleadoID+ " and Activo=1 and  FechaHora BETWEEN '" + sMes + "01000001' and '" + sMes + "31235959'";
                String q1 = "select 'NoArticulos' as Tipo,count( NoArticulos) as Cantidad from (SELECT PreVenta_Mayoreo_Detalle.ArticulosID as NoArticulos from PreVenta_Mayoreo_Detalle,PreVenta_Mayoreo where PreVenta_Mayoreo_Detalle.PreVenta_MayoreoID=PreVenta_Mayoreo.PreVenta_MayoreoID and EmpleadosID=" + sEmpleadoID + " and Activo=1 and  FechaHora BETWEEN '" + sMes + "01000001' and '" + sMes + "31235959' GROUP by PreVenta_Mayoreo_Detalle.ArticulosID)";
                String q2 = "SELECT PreVenta_Mayoreo_Detalle.ArticulosID, sum(PreVenta_Mayoreo_Detalle.Cantidad) as Sum_Cantidad,Articulos.Codigo,Articulos.Nombre from PreVenta_Mayoreo_Detalle,PreVenta_Mayoreo,Articulos where PreVenta_Mayoreo_Detalle.ArticulosID=Articulos.ArticulosID and PreVenta_Mayoreo_Detalle.PreVenta_MayoreoID=PreVenta_Mayoreo.PreVenta_MayoreoID and EmpleadosID=" + sEmpleadoID + " and PreVenta_Mayoreo.Activo=1 and  PreVenta_Mayoreo.FechaHora BETWEEN '" + sMes + "01000001' and '" + sMes + "31235959' GROUP by PreVenta_Mayoreo_Detalle.ArticulosID,Articulos.Codigo,Articulos.Nombre order by Sum_Cantidad Desc  limit 1";


                System.Data.DataSet ds = qryToDataSet(q);
                System.Data.DataSet ds1 = qryToDataSet(q1);
                System.Data.DataSet ds2 = qryToDataSet(q2);

               

                if (ds1.Tables.Count > 0)
                {
                    if (ds.Tables[0].Rows.Count > 0)
                    {
                        ds.Tables[0].ImportRow(ds1.Tables[0].Rows[0]);
                    }
                }

                if (ds2.Tables.Count > 0)
                {
                    if (ds2.Tables[0].Rows.Count > 0) { 
                    System.Data.DataRow r = ds.Tables[0].NewRow();

                    r["Tipo"] ="+"+ds2.Tables[0].Rows[0]["Nombre"].ToString();
                    r["Cantidad"] = ds2.Tables[0].Rows[0]["Sum_Cantidad"].ToString();

                    ds.Tables[0].Rows.Add(r);
                }
                }



                if (ds.Tables.Count > 0)
                {


                    System.Xml.XmlElement xmlElement = Serialize(ds.Tables[0]);

                    return xmlElement.OuterXml.ToString();

                }

            }
            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "obtenerInfoEstatisticas:"+ ex.Message+ex.StackTrace);

            }

            return "";
        }
        //
        [WebMethod(Description = "Regresa el valor del tipo de cambio actual Compras")]
        public String obtenerTCCompras(String sSucursalID )
        {

            try
            {
                String q = "SELECT TCVenta from DatosSucursal where SucursalesID="+ sSucursalID;


                System.Data.DataSet ds = qryToDataSet(q);


                if (ds.Tables.Count > 0)
                {


                    System.Xml.XmlElement xmlElement = Serialize(ds.Tables[0]);

                    return xmlElement.OuterXml.ToString();

                }

            }
            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "obtenerTCCompras:" + ex.Message + ex.StackTrace);

            }

            return "";
        }
        [WebMethod(Description = "Version Altual de MYRAPP")]
        public String versionAppMYR()
        {

      

            return "1.0.0.16" ;
        }
        [WebMethod(Description = "Regresa un xml con los Estados")]
        public String Estados()
        {

            try
            {
                String q = "SELECT * from Estados order by Nombre";


                System.Data.DataSet ds = qryToDataSet(q);


                if (ds.Tables.Count > 0)
                {


                    System.Xml.XmlElement xmlElement = Serialize(ds.Tables[0]);

                    return xmlElement.OuterXml.ToString();

                }

            }
            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "Estados:" + ex.Message + ex.StackTrace);

            }

            return "";
        }
        [WebMethod(Description = "Regresa un xml con las Ciudades")]
        public String Ciudades()
        {

            try
            {
                String q = "SELECT * from Ciudades order by Nombre";


                System.Data.DataSet ds = qryToDataSet(q);


                if (ds.Tables.Count > 0)
                {


                    System.Xml.XmlElement xmlElement = Serialize(ds.Tables[0]);

                    return xmlElement.OuterXml.ToString();

                }

            }
            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "Ciudades:" + ex.Message + ex.StackTrace);

            }

            return "";
        }


        [WebMethod(Description = "Validar el rfc si esta agregado o no")]
        public String RFCExiste(String sRFC)
        {
            String sCodigo = "";

            try
            {
                String q = "SELECT Codigo from Clientes where RFC='"+sRFC+"'";


                System.Data.DataSet ds = qryToDataSet(q);


                if (ds.Tables.Count > 0)
                {

                    

                    sCodigo = ds.Tables[0].Rows[0]["Codigo"].ToString();


                }

            }
            catch (Exception ex)

            {
                sCodigo = "";
               System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "RFCExiste:" + ex.Message + ex.StackTrace);

            }

            return sCodigo;
        }

        [WebMethod(Description = "Registro de Cliente")]
        public String HAddCliente(String sCliente)
        {
            Boolean bBandera = false;
            String[] arr;
            String qry;
            try
            {
                arr = sCliente.Split('|');

                // version 2  para  facturacion
                qry = "Insert into Clientes (RFC,Codigo,Telefono,CiudadesID,EstadosID,NoTelefono,SucursalesID,Activo,Facturacion,PersonaFisica,Nombre,NombreCompleto,ApellidoPaterno,ApellidoMaterno,Direccion,CorreoElectronico,NombreComercial,Calle,Colonia,NumeroInterior,NumeroExterior,CP,Localidad,Celular,Notas,InternalVersion,CorreoAlterno,EsMayoreo,ClientesID,Latitud,Longitud) " +
                                               " Values (";

                /*
                 * 0=NOmbre
                 * 1 apellido paterno
                 * 2 Apellido Materno
                 * 3= Nombre completo
                 * 4= telofono
                 * 5=codigo
                 * 6= requiere factura
                 * 7= persona fisicas
                 * 8= Nombre comercial
                 * 9= calle
                 * 10= numext
                 * 11= numint
                 * 12= colonia
                 * 13=cp
                 * 14=cel
                 * 15= correo
                 * 16= correo alterno
                 * 17= estadosid
                 * 18= ciudadid
                 * 19=rfc
                 * 20=direccion
                 * 21= notas
                 * 22= localidad
                 * 23=ClientesID
                 * 24= latitud
                 * 25= longitud
                 * */
                // RFC
                qry += "'" + arr[19] + "'";
                //Codigo
                qry += ",'" + arr[5] + "'";
                // Telefono
                qry += ",'" + arr[4] + "'";
                //CiudadesID
                qry += ",'" + arr[18] + "'";
                //EstadosID
                qry += ",'" + arr[17] + "'";
                //NoTelefono
                qry += ",'" + arr[14] + "'";
                //SucursalesID
                qry +=",24";
                //Activo
                qry += ",1";
                //Facturacion
                qry += "," + arr[6] + "";
                //PersonaFisica
                qry += "," + arr[7] + "";
                //Nombre
                qry += ",'" + arr[0] + "'";
                //NombreCompleto
                qry += ",'" + arr[3] + "'";
                //ApellidoPaterno
                qry += ",'" + arr[1] + "'";
                //ApellidoMaterno
                qry += ",'" + arr[2] + "'";
                //Direccion
                qry += ",'" + arr[20] + "'";
                //CorreoElectronico
                qry += ",'" + arr[15] + "'";
                //NombreComercial
                qry += ",'" + arr[8] + "'";
                //Calle
                qry += ",'" + arr[9] + "'";
                //Colonia
                qry += ",'" + arr[12] + "'";
                //NumeroInterior
                qry += ",'" + arr[11] + "'";
                //NumeroExterior
                qry += ",'" + arr[10] + "'";
                //CP
                qry += ",'" + arr[13] + "'";
                //Localidad
                qry += ",'" + arr[22] + "'";
                //Celular
                qry += ",'" + arr[14] + "'";
                //Notas
                qry += ",'" + arr[21] + "'";
                //InternalVersion
                qry += ",1";
                //CorreoAlterno
                qry += ",'" + arr[16] + "'";
                //EsMayoreo
                qry += ",1";

                

                qry +=","+siguienteFolio("Clientes") ;
                qry += ",'"+ arr[24] + "'";
                qry += ",'" + arr[25] + "'";


                qry += ")";

               // System.IO.File.WriteAllText(@"C:\sXML\QRY_" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", qry);

                bBandera = qryInsertUpdate(qry);

            }
            catch (Exception ex)
            {
                bBandera = false;
  System.IO.File.WriteAllText(@"C:\sXML\Registro Cliente_" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", ex.Message);            
            }

            if (bBandera) {
                return "1";
            } else {
                return "";
            }


           

        }

        private int siguienteFolio(String sArchivo) {

            String q = "SELECT * from ConsecutivoFolio where Archivo='"+sArchivo+"'";
            int nConsecutivo = 1;
            int id = 0;
            System.Data.DataSet ds = qryToDataSet(q);


            if (ds.Tables.Count > 0)
            {
                id= int.Parse(ds.Tables[0].Rows[0]["ConsecutivoFolioID"].ToString());
                nConsecutivo = int.Parse(ds.Tables[0].Rows[0]["Consecutivo"].ToString());
                nConsecutivo += 1;

                q = "Update ConsecutivoFolio set Consecutivo=" + nConsecutivo + " where ConsecutivoFolioID=" + id;
                //Actualizar
            Boolean    bBandera = qryInsertUpdate(q);

                nConsecutivo = nConsecutivo * 1000;
                //id de la sucurdal
                nConsecutivo += 24;
                   

                if (bBandera)
                {


                }
                else {
                    nConsecutivo = -1;
                }

            }


            return nConsecutivo;
        }

        [WebMethod(Description = "Guarda la posicion del dispositivo")]
        public Boolean HAddPosicion(String sEmpleadoID, String latr, String lon,String sAPP,String sIMEI,String sNota,String Fhora) {

            // se tien que crear una tabla  para k
            //almacenara el registro
            //LogPosicionesGPS
            //LogPosicionesGPSID
            //EmpleadoID
            //Latitud
            //Logitud
            //Dispositivo
            //Notas
            Boolean bBandera=false;
            String q = "Insert Into LogGPS (APP,FechaHora,Latitud,Longitud,IMEI,EmpleadosID,Notas) values ("+
                "'"+sAPP+"',"+
                  "'" + Fhora + "',"+
                  "'" + latr + "',"+
                   "'" + lon + "'," +
                    "'" + sIMEI + "'," +
                     "" + sEmpleadoID+ "," +
                      "'" + sNota + "'"
                + ")";
         //   System.IO.File.WriteAllText(@"C:\sXML\HAddPosicion_" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", q);


            try
            {
                bBandera = false;

                bBandera = qryInsertUpdate(q);


            }
            catch(Exception ex) { 
                System.IO.File.WriteAllText(@"C:\sXML\HAddPosicion_" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", ex.Message+"\n"+ex.StackTrace);
            }


            return bBandera;

        }
        [WebMethod(Description = "Obtener porcentajes por cliente por lineas")]
        public String EnlistarMayoreoClientePorcentaje()
        {
           

            try
            {
                String q = "SELECT Mayoreo_Cliente_Porcentaje.ClientesID,Mayoreo_Cliente_Porcentaje.ProcentajeMayoreoID,Mayoreo_Cliente_Porcentaje.LineasID,Lineas.NombreLinea from Mayoreo_Cliente_Porcentaje,Lineas where Mayoreo_Cliente_Porcentaje.LineasID=Lineas.LineasID order by Lineas.NombreLinea,Mayoreo_Cliente_Porcentaje.ClientesID";


                System.Data.DataSet ds = qryToDataSet(q);

                if (ds.Tables.Count > 0)
                {


                    System.Xml.XmlElement xmlElement = Serialize(ds.Tables[0]);

                    return xmlElement.OuterXml.ToString();

                }


            }
            catch (Exception ex)

            {
              
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "EnListarMayoreoClientePorcentaje:" + ex.Message + ex.StackTrace);

            }

            return "";
        }

        [WebMethod(Description = "Regresa un xml la informacion de Tabla")]
        public String obtenerInfoReplicarcion(String sTabla,String sID)
        {

            try
            {
                String q = "SELECT * from "+sTabla+" where "+sTabla+"ID in (" + sID+")";


                System.Data.DataSet ds = qryToDataSet(q);


                if (ds.Tables.Count > 0)
                {


                    System.Xml.XmlElement xmlElement = Serialize(ds.Tables[0]);

                    return xmlElement.OuterXml.ToString();

                }

            }
            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "obtenerInfoReplicarcion:" + ex.Message + ex.StackTrace);

            }

            return "";
        }
        [WebMethod(Description = "Actualizar articulos app nenas separado por |")]
        public Boolean articulosActivosAPP(String sArticulosID,String sSucursalesID)
        {

          
            Boolean bBandera = false;
            String q = "Delete from AppArticulosDisponibilidad ";
            String[] arr;
            arr = sArticulosID.Split('|');
            //Eliminar registro de la sucursal

            



            try
            {
                bBandera = false;

                bBandera = qryInsertUpdate(q);


                if (bBandera)
                {

                    // inseratr los nuevos articulos

                    q = "";

                    /*  foreach (string sID in arr)
                      {

                          if (sID.Length > 0) {

                              q = "Insert into AppArticulosDisponibilidad (AppArticulosDisponibilidadID,Fecha,SucursalesID,ArticulosID,Activo,InternalVersion) values ("+

                                siguienteFolio("AppArticulosDisponibilidad") +","  +
                                "'',"+
                                sSucursalesID+","+
                                sID+","+
                                "1,"+
                                "1"+
                               ")";

                                 qryInsertUpdate(q);

                          }



                      }
                      */

                    DateTime d;
                    foreach (string sID in arr)
                    {
                        try
                        {

                            if (sID.Length > 0)
                            {

                                d = DateTime.Now;

                                q = "Insert into AppArticulosDisponibilidad (AppArticulosDisponibilidadID,Fecha,SucursalesID,ArticulosID,Activo,InternalVersion) values (" +

                                   d.ToString("yyMMddHHmmssfff") + "," +
                                  "''," +
                                    "2," +
                                  sID + "," +
                                  "1," +
                                  "1" +
                                 ");";

                                qryInsertUpdate(q);

                                q = "Insert into AppArticulosDisponibilidad (AppArticulosDisponibilidadID,Fecha,SucursalesID,ArticulosID,Activo,InternalVersion) values (" +

                                 d.ToString("yyMMddHHmmssfff") + "," +
                                "''," +
                                  "16," +
                                sID + "," +
                                "1," +
                                "1" +
                               ");";

                                qryInsertUpdate(q);

                                q = "Insert into AppArticulosDisponibilidad (AppArticulosDisponibilidadID,Fecha,SucursalesID,ArticulosID,Activo,InternalVersion) values (" +

                            d.ToString("yyMMddHHmmssfff") + "," +
                           "''," +
                            "13," +
                           sID + "," +
                           "1," +
                           "1" +
                          ");";

                                qryInsertUpdate(q);

                                q = "Insert into AppArticulosDisponibilidad (AppArticulosDisponibilidadID,Fecha,SucursalesID,ArticulosID,Activo,InternalVersion) values (" +

                      d.ToString("yyMMddHHmmssfff") + "," +
                     "''," +
                      "6," +
                     sID + "," +
                     "1," +
                     "1" +
                    ");";

                                qryInsertUpdate(q);

                            }






                        }
                        catch (Exception ex)
                        {
                            System.IO.File.WriteAllText(@"C:\sXML\articulosActivosAPP_" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", ex.Message + "\n" + ex.StackTrace);
                        }


                    }


                }
    }
            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\articulosActivosAPP_" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", ex.Message + "\n" + ex.StackTrace);
            }


            return bBandera;

        }
        [WebMethod(Description = "Regresa un xml la informacion de Tabla")]
        public String obtenerHistorialEntradasPedidos(String SucursalesID,String sFini,String sFFin,String sArticuloID)
        {

            try
            {
                String q = @"SELECT 
	Pedidos.PedidosID AS PedidosID,	
	Pedidos.SucursalesID AS SucursalesID,	
	Pedidos.SucursalAlmacenID AS SucursalAlmacenID,	
	Pedidos.NoPedido AS NoPedido,	
	Pedidos.FechaRecibido AS FechaRecibido,	
	Pedidos.FechaSurtido AS FechaSurtido,	
	Pedidos.Total AS Total,	
	Pedidos.CajasSurtidas AS CajasSurtidas,	
	Pedidos.ArticulosSurtidos AS ArticulosSurtidos,	
	Empleados.NombreCompleto AS NombreCompleto,	
	Empleados.EmpleadosID AS EmpleadosID,	
	Sucursales.Nombre AS NombreSucursal,	
	Pedidos.Estatus_PedidosID AS Estatus_PedidosID,	
	Estatus_Pedidos.Nombre AS NombreEstatus,	
	Pedidos.Notas AS Notas
FROM 
	Sucursales,	
	Pedidos,	
	Empleados,	
	Estatus_Pedidos
WHERE 
	Sucursales.SucursalesID = Pedidos.SucursalesID
	AND		Pedidos.EmpleadoSurtioID = Empleados.EmpleadosID
	AND		Pedidos.Estatus_PedidosID = Estatus_Pedidos.Estatus_PedidosID
	AND
	(
		Pedidos.SucursalesID = "+ SucursalesID + @"
		AND Pedidos.Estatus_PedidosID = 3 
        AND Pedidos.FechaRecibido BETWEEN '"+sFini+@"'
         AND '"+sFFin+@"'

	";


                if (sArticuloID.Length > 2) {

                    q += @"  and Pedidos.PedidosID in (SELECT Pedidos.PedidosID from pedidos,Pedidos_Articulos 
                    where Pedidos.PedidosID = Pedidos_Articulos.PedidosID
                    and Pedidos.FechaRecibido BETWEEN '"+sFini+@"'  and '"+sFFin+@"'
                    and Pedidos.Estatus_PedidosID = 3 and Pedidos.SucursalesID = "+SucursalesID+@" and Pedidos_Articulos.ArticulosID = "+ sArticuloID+")";


                }

                q += ")";




                System.Data.DataSet ds = qryToDataSet(q);


                if (ds.Tables.Count > 0)
                {


                    System.Xml.XmlElement xmlElement = Serialize(ds.Tables[0]);

                    return xmlElement.OuterXml.ToString();

                }

            }
            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "obtenerHistorialEntradasPedidos:" + ex.Message + ex.StackTrace);

            }

            return "";
        }
        [WebMethod(Description = "Regresa un xml la informacion de Tabla")]
        public String obtenerHistorialEntradas(String SucursalesID, String sFini, String sFFin, String sArticuloID,String sTipoMovimientoID,String sRecepcionLocal,String sDolares,String sEstatus,String sFamiliasID,String sLineasID,String sFolioMov,String sEntradasID,String sReferencia,String sProveedorID,String sOrigenID,String sTieneBonificado)
        {
            String q = "";
            try
            {
                q = @"SELECT DISTINCT 
Entradas.EntradasID AS EntradasID,	
Entradas.ProveedoresID AS ProveedoresID,	
Entradas.TiposMovimientosID AS TiposMovimientosID,	
Entradas.FechaEntrada AS FechaEntrada,	
Entradas.Referencia AS Referencia,	
Entradas.Subtotal AS Subtotal,	
Entradas.IVA AS IVA,	
Entradas.Importe AS Importe,	
Entradas.Notas AS Notas,	
Proveedores.Nombre AS Nombre,	
TiposMovimientos.DescripcionMovimiento AS DescripcionMovimiento,	
Entradas.FolioMovimiento AS FolioMovimiento,	
Entradas.Estatus_MovimientosID AS Estatus_MovimientosID,	
Estatus_Movimientos.Nombre AS EstatusNombre,	
Entradas.EmpleadoGeneroID AS EmpleadoGeneroID,	
Destinos.Nombre AS Nombre_OrigenDestino,	
Entradas.TieneBonificacion AS TieneBonificacion,	
Entradas.OrigenID AS OrigenID,	
Entradas.MotivoCancelacion AS MotivoCancelacion,
Entradas.ImporteDescuento AS ImporteDescuento,
Entradas.CantidadEntrada AS CantidadEntrada,
Entradas.Dolares AS Dolares
FROM 
Destinos
RIGHT OUTER JOIN
(
Estatus_Movimientos
LEFT OUTER JOIN
(
Proveedores
RIGHT OUTER JOIN
(
TiposMovimientos
INNER JOIN
(
Familias
LEFT OUTER JOIN
(
Entradas
LEFT OUTER JOIN
(
Articulos
INNER JOIN
EntradasArticulos
ON Articulos.ArticulosID = EntradasArticulos.ArticulosID
)
ON Entradas.EntradasID = EntradasArticulos.EntradasID
)
ON Familias.FamiliasID = Articulos.FamiliasID
)
ON TiposMovimientos.TiposMovimientosID = Entradas.TiposMovimientosID
)
ON Proveedores.ProveedoresID = Entradas.ProveedoresID
)
ON Entradas.Estatus_MovimientosID = Estatus_Movimientos.Estatus_MovimientosID
)
ON Destinos.DestinosID = Entradas.OrigenID
WHERE 
(
	Entradas.SucursalesID ="+ SucursalesID + @"

AND	Entradas.FechaEntrada BETWEEN '"+sFini+@"' AND '"+sFFin+@"' ";


                if (sTipoMovimientoID.Length > 1)
                {

                    q += "  AND	Entradas.TiposMovimientosID ="+sTipoMovimientoID;
                    
                }
              
                if (sTipoMovimientoID.Length > 1)
                {

                    q += "  AND Entradas.RecepcionLocal = " + sRecepcionLocal;

                }
                if (sDolares.Length > 1)
                {

                    q += " AND Entradas.Dolares = " + sDolares;

                }
            
                if (sEstatus.Length > 1)
            {

                q += " AND	Entradas.Estatus_MovimientosID  = " + sEstatus;

            }
                if (sFamiliasID.Length > 1)
                {

                    q += " AND	Articulos.FamiliasID = " + sFamiliasID;

                }
                if (sLineasID.Length > 1)
                {

                    q += " AND	Articulos.LineaID = " + sLineasID;

                }
                if (sFolioMov.Length > 1)
                {

                    q += " AND	Entradas.FolioMovimiento = " + sFolioMov;

                }
                if (sEntradasID.Length > 1)
                {

                    q += " AND	Entradas.EntradasID  = " + sEntradasID;

                }
                if (sReferencia.Length > 1)
                {

                    q += " AND	Entradas.Referencia  = '" + sReferencia+@"'";

                }
                if (sProveedorID.Length > 1)
                {

                    q += " AND	Entradas.ProveedoresID = " + sProveedorID;

                }
                if (sArticuloID.Length > 1)
                {

                    q += " AND	EntradasArticulos.ArticulosID  = " + sArticuloID;

                }
                if (sOrigenID.Length > 1)
                {

                    q += " AND	Entradas.OrigenID= " + sOrigenID;

                }
                if (sTieneBonificado.Length > 0)
                {

                    q += " AND	Entradas.TieneBonificacion =  " + sTieneBonificado;

                }



                q += " )  ORDER BY EntradasID ASC ";

             


                System.Data.DataSet ds = qryToDataSet(q);


                if (ds.Tables.Count > 0)
                {


                    System.Xml.XmlElement xmlElement = Serialize(ds.Tables[0]);

                    return xmlElement.OuterXml.ToString();

                }

            }
            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "obtenerHistorialEntradasPedidos:" + ex.Message + ex.StackTrace+"\n"+q);

            }

            return "";
        }
        [WebMethod(Description = "Regresa un xml la informacion de Tabla")]
        public String obtenerArticulosEntradas( String sEntradasID)
        {
            String q = "";
            try
            {
                q = @"SELECT 
	EntradasArticulos.EntradasID AS EntradasID,	
	EntradasArticulos.EntradasArticulosID AS EntradasArticulosID,	
	EntradasArticulos.ArticulosID AS ArticulosID,	
	EntradasArticulos.Cantidad AS Cantidad,	
	EntradasArticulos.CostoSIVA AS CostoSIVA,	
	EntradasArticulos.Costo AS Costo,	
	Articulos.Nombre AS Nombre,	
	EntradasArticulos.ImporteCosto AS ImporteCosto,	
	EntradasArticulos.ImporteIVACosto AS ImporteIVACosto,	
	Articulos.Codigo AS Codigo,	
	EntradasArticulos.Devuelto AS Devuelto,	
	EntradasArticulos.CantidadRecibida AS CantidadRecibida,	
	EntradasArticulos.CantidadBonificacion AS CantidadBonificacion,	
	EntradasArticulos.CantidadBonificacionRecibida AS CantidadBonificacionRecibida,	
	EntradasArticulos.PresentacionesID AS PresentacionesID,	
	EntradasArticulos.presentacionesID_Empaque AS presentacionesID_Empaque
FROM 
	Articulos,	
	EntradasArticulos
WHERE 
	Articulos.ArticulosID = EntradasArticulos.ArticulosID
	AND
	(
		EntradasArticulos.EntradasID ="+sEntradasID+")";


              

                System.Data.DataSet ds = qryToDataSet(q);


                if (ds.Tables.Count > 0)
                {


                    System.Xml.XmlElement xmlElement = Serialize(ds.Tables[0]);

                    return xmlElement.OuterXml.ToString();

                }

            }
            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "obtenerEntradaArticulos:" + ex.Message + ex.StackTrace + "\n" + q);

            }

            return "";
        }
        [WebMethod(Description = "Regresa un xml la informacion de Tabla")]
        public String obtenerPedidosDetalleQRY(String sPedidosID)
        {
            String q = "";
            try
            {
                q = @"SELECT 
	EntradasArticulos.EntradasID AS EntradasID,	
	EntradasArticulos.EntradasArticulosID AS EntradasArticulosID,	
	EntradasArticulos.ArticulosID AS ArticulosID,	
	EntradasArticulos.Cantidad AS Cantidad,	
	EntradasArticulos.CostoSIVA AS CostoSIVA,	
	EntradasArticulos.Costo AS Costo,	
	Articulos.Nombre AS Nombre,	
	EntradasArticulos.ImporteCosto AS ImporteCosto,	
	EntradasArticulos.ImporteIVACosto AS ImporteIVACosto,	
	Articulos.Codigo AS Codigo,	
	EntradasArticulos.Devuelto AS Devuelto,	
	EntradasArticulos.CantidadRecibida AS CantidadRecibida,	
	EntradasArticulos.CantidadBonificacion AS CantidadBonificacion,	
	EntradasArticulos.CantidadBonificacionRecibida AS CantidadBonificacionRecibida,	
	EntradasArticulos.PresentacionesID AS PresentacionesID,	
	EntradasArticulos.presentacionesID_Empaque AS presentacionesID_Empaque
FROM 
	Articulos,	
	EntradasArticulos
WHERE 
	Articulos.ArticulosID = EntradasArticulos.ArticulosID
	AND
	(
		EntradasArticulos.EntradasID = "+sPedidosID+")";




                System.Data.DataSet ds = qryToDataSet(q);


                if (ds.Tables.Count > 0)
                {


                    System.Xml.XmlElement xmlElement = Serialize(ds.Tables[0]);

                    return xmlElement.OuterXml.ToString();

                }

            }
            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "obtenerEntradaArticulos:" + ex.Message + ex.StackTrace + "\n" + q);

            }

            return "";
        }
        [WebMethod(Description = "Regresa un xml la informacion de Tabla")]
        public String obtenerArticulosMayoreo(String sFecha)
        {
            String q = "";
            try
            {
                q = @"SELECT 
	Ventas.FechaVenta AS FechaVenta,	
	Ventas.IPInventarioAfecto AS IPInventarioAfecto,	
	Ventas.Cancelada AS Cancelada,	
	Ventas.SucursalesID AS SucursalesID,	
	Ventas_Articulos.ArticulosID AS ArticulosID,	
	Articulos.Codigo AS Codigo,	
    Articulos.ImpuestosID as ImpuestosID,
    Articulos.PermitirDecimales as PermitirDecimales,
	Ventas_Articulos.LineaID AS LineaID,	
	Ventas_Articulos.Cantidad AS Cantidad,	
	Ventas_Articulos.Importe AS Importe,
	ventas.VentasID
FROM 
	Ventas,	
	Ventas_Articulos,	
	Articulos
WHERE 
	Ventas.VentasID = Ventas_Articulos.VentasID
	AND		Ventas_Articulos.ArticulosID = Articulos.ArticulosID
	AND
	(
		Ventas.Cancelada = 0
		AND	Ventas.FechaVenta ='" + sFecha+@"'
		AND	LENGTH(Ventas.IPInventarioAfecto )  > 0
	)";




                System.Data.DataSet ds = qryToDataSet(q);


                if (ds.Tables.Count > 0)
                {


                    System.Xml.XmlElement xmlElement = Serialize(ds.Tables[0]);

                    return xmlElement.OuterXml.ToString();

                }

            }
            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "obtenerArticulosMayoreo:" + ex.Message + ex.StackTrace + "\n" + q);

            }

            return "";
        }
        [WebMethod(Description = "Artualiza ventas de Mayoreo transferencias salida")]
        public Boolean UpdateVentasMRY( String sFecha)
        {

            Boolean bBandera = false;
            String q = @"UPDATE 
	Ventas
SET
	IPInventarioAfecto =''
WHERE 
	Ventas.IPInventarioAfecto ='192.168.10.253'
	 AND Ventas.FechaVenta="+sFecha;
            //   System.IO.File.WriteAllText(@"C:\sXML\HAddPosicion_" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", q);


            try
            {
                bBandera = false;

                bBandera = qryInsertUpdate(q);


            }
            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\pdateVentasMRY_" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", ex.Message + "\n" + ex.StackTrace);
            }


            return bBandera;

        }
        [WebMethod(Description = "Enviar archivo para que lo importe el servicio")]
        public Boolean EnviarArchivoOfertas(byte[] f, string fileName)
        {

          //  el argumento de la matriz de bytes contiene el contenido del archivo
            // el argumento de cadena contiene el nombre y la extensión
            // del archivo pasado en la matriz de bytes 
            try
            {

              if(  System.IO.Directory.Exists("C:/sXML/importacion /")==false){
                    System.IO.Directory.CreateDirectory("C:/sXML/importacion/");

                }

                // instance a memory stream and pass the  
                // byte array to its constructor  
             System.IO.MemoryStream ms = new System.IO.MemoryStream(f);
                // instance a filestream pointing to the  
                // storage folder, use the original file name  
                // to name the resulting file  
                System.IO.FileStream fs = new System.IO.FileStream("C:/sXML/importacion/" + fileName, System.IO.FileMode.Create);
                // write the memory stream containing the original  
                // file as a byte array to the filestream  
                ms.WriteTo(fs);
                // clean up  
                ms.Close();
                fs.Close();
                fs.Dispose();
                // return OK if we made it this far  
                return true;

            }
            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\EnviarArchivoOfertas_" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", ex.Message + "\n" + ex.StackTrace);
            }


            return false;

        }
        [WebMethod(Description = " qry de Historial")]
        public String Qry_INV_PedidosHistoriales(string pSucursalesID,string pFechaRInicio,string pFechaRFinal, string pFechaSInicio, string pFechaSFinal, string pSucursalAlmacenID,string pEstatus_PedidosID)
        {
            String q = "";
            try
            {
                q = @" SELECT 
	Pedidos.PedidosID AS PedidosID,	
	Pedidos.SucursalesID AS SucursalesID,	
	Pedidos.SucursalAlmacenID AS SucursalAlmacenID,	
	Pedidos.NoPedido AS NoPedido,	
	Pedidos.FechaRecibido AS FechaRecibido,	
	Pedidos.FechaSurtido AS FechaSurtido,	
	Pedidos.Total AS Total,	
	Pedidos.CajasSurtidas AS CajasSurtidas,	
	Pedidos.ArticulosSurtidos AS ArticulosSurtidos,	
	Empleados.NombreCompleto AS NombreCompleto,	
	Empleados.EmpleadosID AS EmpleadosID,	
	Sucursales.Nombre AS NombreSucursal,	
	Pedidos.Estatus_PedidosID AS Estatus_PedidosID,	
	Estatus_Pedidos.Nombre AS NombreEstatus,	
	Pedidos.Notas AS Notas
  FROM 
	Sucursales,	
	Pedidos,	
	Empleados,	
	Estatus_Pedidos
 WHERE 
	Sucursales.SucursalesID = Pedidos.SucursalesID
	AND		Pedidos.EmpleadoSurtioID = Empleados.EmpleadosID
	AND		Pedidos.Estatus_PedidosID = Estatus_Pedidos.Estatus_PedidosID
	AND
	(
			Pedidos.FechaSurtido BETWEEN '" + pFechaSInicio + @"' AND '" + pFechaSFinal + @"' ";

                if (pFechaRInicio.Length>0 && pFechaRFinal.Length>0) {
                
                    q += " AND Pedidos.FechaRecibido BETWEEN '" + pFechaRInicio + "' AND '" + pFechaRFinal+"' ";
                }


                if (pSucursalesID.Length>0) {

                    q += " AND Pedidos.SucursalesID = " + pSucursalesID;
                }

	           if (pSucursalAlmacenID.Length > 0) {


                    q += " AND Pedidos.SucursalAlmacenID = " + pSucursalAlmacenID;
                }

                if (pEstatus_PedidosID.Length > 0)
                {
                    q += "  AND Pedidos.Estatus_PedidosID IN (" + pEstatus_PedidosID+") ";
                }

               


            q+=  " )";

           //    System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "Qry" + q);


                System.Data.DataSet ds = qryToDataSet(q);


                if (ds.Tables.Count > 0)
                {


                    System.Xml.XmlElement xmlElement = Serialize(ds.Tables[0]);

                    return xmlElement.OuterXml.ToString();

                }

            }
            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "Qry_INV_PedidosHistoriales_X_Fechas:" + ex.Message + ex.StackTrace + "\n" + q);

            }

            return "";
        }

        // 
        [WebMethod(Description = "QRY_Salidas_X_ArticuloID_S_Fecha")]
        public String QRY_Salidas_X_ArticuloID_S_Fecha(string pArticulosID,string pFechaInicial,string pFechaFInal,string pDestinosID,string pTipoMovimientoID,string pSalidasID,string pSucursalesID,string pfolioMovimiento,string pEstatusMovimientoID,string pFamiliasID,string pLineasID,string pProveedoresID,string pPendienteAplicarNC)
        {
            String q = "";
            try
            {
                q = @" SELECT DISTINCT 
	Salidas.SalidasID AS SalidasID,	
	Salidas.DestinosID AS DestinosID,	
	Salidas.TiposMovimientosID AS TiposMovimientosID,	
	Salidas.FechaSalida AS FechaSalida,	
	Salidas.Subtotal AS Subtotal,	
	Salidas.IVA AS IVA,	
	Salidas.Importe AS Importe,	
	Salidas.Notas AS Notas,	
	Destinos.Nombre AS Nombre,	
	TiposMovimientos.DescripcionMovimiento AS DescripcionMovimiento,	
	Salidas.FolioMovimiento AS FolioMovimiento,	
	Salidas.Estatus_MovimientosID AS Estatus_MovimientosID,	
	Estatus_Movimientos.Nombre AS EstatusNombre,	
	Proveedores.Nombre AS ProveedorNombre,	
	Salidas.EmpleadoGeneroID AS EmpleadoGeneroID,	
	Salidas.ProveedoresID AS ProveedoresID,	
	Salidas.Referencia AS Referencia,	
	Salidas.MotivoCancelacion AS MotivoCancelacion,	
	Salidas.Recibido AS Recibido,	
	Salidas.CantidadSalida AS CantidadSalida,	
	Salidas.PendienteAplicarNC AS PendienteAplicarNC,	
	Salidas.FolioRecepcionID AS FolioRecepcionID
FROM 
	Estatus_Movimientos
	INNER JOIN
	(
		Articulos
		INNER JOIN
		(
			(
				TiposMovimientos
				INNER JOIN
				(
					Destinos
					RIGHT OUTER JOIN
					(
						Proveedores
						RIGHT OUTER JOIN
						Salidas
						ON Proveedores.ProveedoresID = Salidas.ProveedoresID
					)
					ON Destinos.DestinosID = Salidas.DestinosID
				)
				ON TiposMovimientos.TiposMovimientosID = Salidas.TiposMovimientosID
			)
			INNER JOIN
			SalidasArticulos
			ON Salidas.SalidasID = SalidasArticulos.SalidasID
		)
		ON Articulos.ArticulosID = SalidasArticulos.ArticulosID
	)
	ON Estatus_Movimientos.Estatus_MovimientosID = Salidas.Estatus_MovimientosID
WHERE 
	(
	Salidas.FechaSalida BETWEEN '"+pFechaInicial+@"' AND '"+pFechaFInal+@"'
AND	Salidas.SucursalesID =" +pSucursalesID;


                if (pArticulosID.Length > 0) {
                    q += " AND	SalidasArticulos.ArticulosID =  "+pArticulosID;
                }

                if (pDestinosID.Length>0) {

                    q+= " AND	Salidas.DestinosID ="+ pDestinosID;
                }


                if (pTipoMovimientoID.Length>0) {

                    q+= " AND	Salidas.TiposMovimientosID ="+pTipoMovimientoID;
                }

                if (pSalidasID.Length>0) {

                    q += "AND	Salidas.SalidasID ="+ pSalidasID;
                }
                if (pfolioMovimiento.Length>0) {

                    q+= "AND	Salidas.FolioMovimiento ="+pfolioMovimiento;
                }

                if (pEstatusMovimientoID.Length>0) {

                    q+= " AND	Salidas.Estatus_MovimientosID ="+pEstatusMovimientoID;
                }
                if (pFamiliasID.Length>0) {
                    q += " AND	Articulos.FamiliasID = " + pFamiliasID;
                }

                if (pLineasID.Length>0) {

                    q+= " AND	Articulos.LineaID ="+pLineasID;
                }
                if (pProveedoresID.Length>0) {
                    q+= "	AND	Salidas.ProveedoresID = "+pProveedoresID;
                }

                if (pPendienteAplicarNC.Length>0) {

                    q+= " AND	Salidas.PendienteAplicarNC ="+pPendienteAplicarNC;
                }
	

  q+=" ) ORDER BY  FechaSalida ASC";

        
                System.Data.DataSet ds = qryToDataSet(q);


                if (ds.Tables.Count > 0)
                {


                    System.Xml.XmlElement xmlElement = Serialize(ds.Tables[0]);

                    return xmlElement.OuterXml.ToString();

                }

            }
            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "QRY_Salidas_X_ArticuloID_S_Fecha:" + ex.Message + ex.StackTrace+ex.InnerException + "\n" + q);

            }

            return "";
        }
        [WebMethod(Description = "regresa el precio de articulo contemplando ofertas (Precio|Nivel)")]
        public String PrecioArticulo(String sArticuloID, String sSucursalesID)
        {
            System.Xml.XmlElement xmlElement;
            String sQry = "select * from Articulos where Activo = 1";
            System.Data.DataSet ds = qryToDataSet(sQry);
            if (ds.Tables.Count > 0)
            {
                xmlElement = Serialize(ds.Tables[0]);
                return xmlElement.OuterXml.ToString();
            }
            return "";
        }


        [WebMethod(Description = "QRY_Salidas_X_ArticuloID_S_Fecha")]
        public String QRY_DetalleSalidas_Por_SalidasID(String pSucursalesID, string pSalidasID)
        {
            String q = "";
            try
            {
                q = @" SELECT 
	Salidas.SucursalesID AS SucursalesID,		
	SalidasArticulos.ArticulosID AS ArticulosID,	
	SalidasArticulos.PresentacionesID AS PresentacionesID,	
	Articulos.LineaID AS LineaID,	
	Articulos.Codigo AS Codigo,	
	Articulos.Nombre AS Nombre,	
	SalidasArticulos.CostoSIVA AS CostoSIVA,	
	SalidasArticulos.Costo AS Costo,	
	SUM(SalidasArticulos.Cantidad) AS sum_Cantidad
FROM 
	Articulos,	
	SalidasArticulos,	
	Salidas
WHERE 
	Articulos.ArticulosID = SalidasArticulos.ArticulosID
	AND		Salidas.SalidasID = SalidasArticulos.SalidasID
	and
	(
			Salidas.SucursalesID ="+pSucursalesID+@"
		AND	SalidasArticulos.SalidasID IN ("+pSalidasID+@")
		
	)
GROUP BY 
	Salidas.SucursalesID,		
	SalidasArticulos.ArticulosID,	
	SalidasArticulos.PresentacionesID,	
	Articulos.LineaID,	
	Articulos.Codigo,	
	Articulos.Nombre,	
	SalidasArticulos.CostoSIVA,	
	SalidasArticulos.Costo
ORDER BY 
	
	Nombre ASC";


               




                System.Data.DataSet ds = qryToDataSet(q);


                if (ds.Tables.Count > 0)
                {


                    System.Xml.XmlElement xmlElement = Serialize(ds.Tables[0]);

                    return xmlElement.OuterXml.ToString();

                }

            }
            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "QRY_DetalleSalidas_Por_SalidasID:" + ex.Message + ex.StackTrace + ex.InnerException + "\n" + q);

            }

            return "";
        }
        [WebMethod(Description = "Obtene los articulos de los pedidos enviados IN")]
        public String obtenerHistorialEntradasPedidosDetalle( String pInPedidosID)
        {

            try
            {
                String q = @"SELECT 
	Pedidos_Articulos.ArticulosID as ArticulosID,
     Articulos.Codigo as Codigo,
   Articulos.Nombre as Nombre,
  sum(Pedidos_Articulos.PiezasSurtidas) as SUM_PiezasSurtidas,
  avG(Pedidos_Articulos.CostoConIvaUnitario) as AVG_CostoCIVA,
  sum( Pedidos_Articulos.ImporteIVACosto) as SUN_ImporteCostoCIVA
FROM 
	
	Pedidos,Pedidos_Articulos,Articulos
		
WHERE 
        Pedidos.PedidosID=Pedidos_Articulos.PedidosID
	and Pedidos_Articulos.ArticulosID=Articulos.ArticulosID
	AND
	(
	
		 Pedidos.Estatus_PedidosID = 3 
        AND Pedidos.PedidosID in (" + pInPedidosID+ @")
) GROUP by 
Pedidos_Articulos.ArticulosID ,
     Articulos.Codigo,
   Articulos.Nombre ";




                System.Data.DataSet ds = qryToDataSet(q);


                if (ds.Tables.Count > 0)
                {


                    System.Xml.XmlElement xmlElement = Serialize(ds.Tables[0]);

                    return xmlElement.OuterXml.ToString();

                }

            }
            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "obtenerHistorialEntradasPedidosDetalle:" + ex.Message + ex.StackTrace);

            }

            return "";
        }
        [WebMethod(Description = "Regresa un xml la informacion de Tabla")]
        public String obtenerArticulosIN_Entradas(String psINEntradasID)
        {
            String q = "";
            try
            {
                q = @"SELECT 
    EntradasArticulos.ArticulosID AS ArticulosID,	
    Articulos.Codigo AS Codigo,
    Articulos.Nombre AS Nombre,	
    sum(EntradasArticulos.CantidadRecibida) as SUM_CantidadRecibida,	
    AVG(EntradasArticulos.Costo) AS AVG_Costo,	
    Sum(	EntradasArticulos.ImporteIVACosto) AS SUM_ImporteIVACosto	
	
    FROM 
	Articulos,	
	EntradasArticulos
    
    WHERE 
	Articulos.ArticulosID = EntradasArticulos.ArticulosID
	AND
	(
		EntradasArticulos.EntradasID IN (" + psINEntradasID + @")
        
    )
    group by
    EntradasArticulos.ArticulosID,
    Articulos.Codigo ,
    Articulos.Nombre	
   ";




                System.Data.DataSet ds = qryToDataSet(q);


                if (ds.Tables.Count > 0)
                {


                    System.Xml.XmlElement xmlElement = Serialize(ds.Tables[0]);

                    return xmlElement.OuterXml.ToString();

                }

            }
            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "obtenerArticulosIN_Entradas:" + ex.Message + ex.StackTrace + "\n" + q);

            }

            return "";
        }

        
        [WebMethod(Description = "Consultar bajas de empleados")]
        public String ConsultarBajas(String FechaInicio, String FechaFinal, String pSucursalesID)
        {
            System.Xml.XmlElement xmlElement;
            String sQry ="";
              try
            {
             sQry = @"SELECT Empleados.PuestosID, Puestos.Nombre AS nombrePuesto, Empleados.DepartamentosID, Departamentos.Descripcion AS nombreDepartamento, Empleados.Activo,
Empleados.Nombre, Empleados.ApellidoPaterno, Empleados.ApellidoMaterno, Empleados.sexo, Empleados.FechaIngreso, Empleados.FechaNacimiento, Empleados.EstadoCivil, Empleados.NivelEstudios,
Historial_Bajas.Fecha_Baja, Historial_Bajas.Comentarios, Historial_Bajas.EmpleadosID,Historial_Bajas.MotivosDeBajaCatalogo 
FROM Historial_Bajas JOIN Empleados on Empleados.EmpleadosID = Historial_Bajas.EmpleadosID 
JOIN Puestos ON Empleados.PuestosID = Puestos.PuestosID 
JOIN Departamentos ON Empleados.DepartamentosID = Departamentos.DepartamentosID WHERE Empleados.SucursalesID = "+pSucursalesID+@" AND 
 Historial_Bajas.Fecha_Baja BETWEEN '"+FechaInicio+@"' AND '"+FechaFinal+"' ORDER BY Fecha_Baja";
             
            
            System.Data.DataSet ds = qryToDataSet(sQry);
            if (ds.Tables.Count > 0)
            {
                xmlElement = Serialize(ds.Tables[0]);
                return xmlElement.OuterXml.ToString();
            }
            }
             catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "ConsultarBajas:" + ex.Message + ex.StackTrace + "\n" + sQry);

            }

            return "";
        }


        [WebMethod(Description = "Total bajas de empleados")]
        public String ConsultarTotalBajas(String FechaInicio, String FechaFinal, String pSucursalesID)
        {
            System.Xml.XmlElement xmlElement;
            String sQry = "";
              try
            {
             sQry = @"Select count(*) As BajasPeriodo
                FROM  Historial_Bajas JOIN Empleados ON Empleados.EmpleadosID = Historial_Bajas.EmpleadosID where  Historial_Bajas.Fecha_Baja 
                BETWEEN '"+FechaInicio+@"' AND '"+FechaFinal+"' AND Empleados.SucursalesID = "+pSucursalesID;
             
            System.Data.DataSet ds = qryToDataSet(sQry);
            if (ds.Tables.Count > 0)
            {
                xmlElement = Serialize(ds.Tables[0]);
                return xmlElement.OuterXml.ToString();
            }
        }

             catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "Total bajas de empleados:" + ex.Message + ex.StackTrace + "\n" + sQry);

            }

            return "";
        }
          [WebMethod(Description = "Total Plantilla")]
        public String ObtenerTotalPlantilla(String pSucursalesID)
        {
            System.Xml.XmlElement xmlElement;
            String sQry = "";
              try
            {
             sQry = @"SELECT sum(Puestos_Sucursales.CantidadEmpleados) AS Plantilla 
             FROM Puestos_Sucursales WHERE Puestos_Sucursales.Activo = 1 AND Puestos_Sucursales.SucursalesID = "+pSucursalesID;
               
            System.Data.DataSet ds = qryToDataSet(sQry);
            if (ds.Tables.Count > 0)
            {
                xmlElement = Serialize(ds.Tables[0]);
                return xmlElement.OuterXml.ToString();
            }
        }

             catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "Obtener Plantilla:" + ex.Message + ex.StackTrace + "\n" + sQry);

            }

            return "";
        }


        [WebMethod(Description = "Devuelve el precio del codigo del articulo, Return vacio sino encuentro")]
        public String PrecioVerificador(String pSucursalesID,String sCodigo)
        {
            System.Xml.XmlElement xmlElement;
            String sQry = "";
            String sArticulosID = "";
            String sDescripcionArticulo = "";
            String sPrecioCIVA = "";
            String bPromocion = "";
            String sPrecioRealCIVA = "";
            try
            {
                //se busca el articulo por el codigo
                sQry = @"SELECT 
	Articulos.ArticulosID AS ArticulosID,	
	Articulos.Nombre AS Nombre,	
	Articulos.Codigo AS Codigo,	
	Articulos.SeVende AS SeVende,	
	Articulos.UsoInterno AS UsoInterno,	
	Articulos.Activo AS Activo,	
	Articulos.UnidadSalida AS UnidadSalida,	
	Articulos.AplicaDescuento AS AplicaDescuento,	
	Articulos.ImpuestosID AS ImpuestosID,	
	Articulos.LineaID AS LineaID,	
	Articulos.PermitirDecimales AS PermitirDecimales,	
	Articulos.EmpaqueEntradaID AS EmpaqueEntradaID,	
	Articulos.EmpaqueSalidaID AS EmpaqueSalidaID,	
	Articulos.EmpaqueEntradaReciboID AS EmpaqueEntradaReciboID,	
	Articulos.PrecioVariable AS PrecioVariable,	
	Articulos.NoVendeNegativo AS NoVendeNegativo,	
	Articulos.SeManufactura AS SeManufactura,	
	Articulos.NoRestarExistencia AS NoRestarExistencia,	
	Articulos.TiposIEPSID AS TiposIEPSID,	
	Articulos.ValorIEPS AS ValorIEPS,	
	Articulos.CantidadIEPS AS CantidadIEPS,	
	Articulos.RespetaPrecioAunqueUtiliceBascula AS RespetaPrecioAunqueUtiliceBascula,	
	Articulos.RotacionInventarioID AS RotacionInventarioID,	
	Articulos.esServicio AS esServicio,	
	Articulos.articuloID_Comision AS articuloID_Comision,	
	Articulos.esComision AS esComision,	
	Articulos.esDeposito AS esDeposito,	
	Articulos.esRetiro AS esRetiro
FROM 
	Articulos
WHERE 
	Articulos.Codigo ="+"'"+sCodigo+"'" ;


                System.Data.DataSet ds = qryToDataSet(sQry);
                if (hayInfoDS(ds))
                {
                    // Se encontro
                    sArticulosID = ds.Tables[0].Rows[0]["ArticulosID"].ToString();
                    sDescripcionArticulo = ds.Tables[0].Rows[0]["Nombre"].ToString();


                }
                else {
                    //se busca el articulo por el codigo de barras
                    sQry = @"SELECT 
	Articulos.ArticulosID AS ArticulosID,	
	Articulos.Nombre AS Nombre,	
	Articulos.Codigo AS Codigo,	
	Articulos.SeVende AS SeVende,	
	Articulos.UsoInterno AS UsoInterno,	
	Articulos.Activo AS Activo,	
	Articulos.UnidadSalida AS UnidadSalida,	
	CodigosBarra_Articulos.CodigoBarra AS CodigoBarra,	
	Articulos.AplicaDescuento AS AplicaDescuento,	
	Articulos.ImpuestosID AS ImpuestosID,	
	Articulos.LineaID AS LineaID,	
	Articulos.PermitirDecimales AS PermitirDecimales,	
	Articulos.EmpaqueEntradaID AS EmpaqueEntradaID,	
	Articulos.EmpaqueSalidaID AS EmpaqueSalidaID,	
	Articulos.EmpaqueEntradaReciboID AS EmpaqueEntradaReciboID,	
	Articulos.PrecioVariable AS PrecioVariable,	
	Articulos.NoVendeNegativo AS NoVendeNegativo,	
	Articulos.SeManufactura AS SeManufactura,	
	Articulos.NoRestarExistencia AS NoRestarExistencia,	
	Articulos.TiposIEPSID AS TiposIEPSID,	
	Articulos.ValorIEPS AS ValorIEPS,	
	Articulos.CantidadIEPS AS CantidadIEPS,	
	Articulos.RespetaPrecioAunqueUtiliceBascula AS RespetaPrecioAunqueUtiliceBascula,	
	Articulos.RotacionInventarioID AS RotacionInventarioID,	
	Articulos.esServicio AS esServicio,
	Articulos.articuloID_Comision AS articuloID_Comision,
	Articulos.esComision AS esComision,
	Articulos.esRetiro AS esRetiro,
	Articulos.esDeposito AS esDeposito
FROM 
	Articulos,	
	CodigosBarra_Articulos
WHERE 
	Articulos.ArticulosID = CodigosBarra_Articulos.ArticulosID
	AND
	(
		CodigosBarra_Articulos.CodigoBarra ="+"'"+sCodigo+"'"+")";
                    ds = null;
                    ds = qryToDataSet(sQry);

                    

                    if (hayInfoDS(ds))
                    {
                        
                        // Se encontro
                        sArticulosID = ds.Tables[0].Rows[0]["ArticulosID"].ToString();
                        sDescripcionArticulo = ds.Tables[0].Rows[0]["Nombre"].ToString();


                    }
                    else {
                        // regresa  vacio si no o encoentro
                        return "";

                    }


                }

                // continuar con el proceso
                //se checa si hay alguna promocion activa para este articul
                sQry = @"SELECT 
	ArticulosPrecios.ArticulosPreciosID AS ArticulosPreciosID,	
	ArticulosPrecios.Precio AS Precio,	
	ArticulosPrecios.IVAPrecio AS IVAPrecio,	
	ArticulosPrecios.PrecioCIVA AS PrecioCIVA,	
	ArticulosPrecios.Costo AS Costo,	
	ArticulosPrecios.CostoCIVA AS CostoCIVA,	
	ArticulosPrecios.ArticulosID AS ArticulosID,	
	ArticulosPrecios.EsPromocion AS EsPromocion,	
	ArticulosPrecios.FechaInicio AS FechaInicio,	
	ArticulosPrecios.FechaFinal AS FechaFinal,	
	ArticulosPrecios.Nivel AS Nivel,	
	ArticulosPrecios.LimiteDeUnidades AS LimiteDeUnidades
FROM 
	ArticulosPrecios
WHERE 
	ArticulosPrecios.ArticulosID = "+sArticulosID+@"
	AND	ArticulosPrecios.SucursalesID ="+ pSucursalesID + @"
	AND	ArticulosPrecios.EsPromocion = 0
	AND	ArticulosPrecios.FechaInicio <= "+ DateTime.Now.ToString("yyyyMMdd") + @"
	AND	ArticulosPrecios.FechaFinal >= "+DateTime.Now.ToString("yyyyMMdd")+@"
	AND	ArticulosPrecios.Activo =0";
                ds = null;
                ds = qryToDataSet(sQry);
                if (hayInfoDS(ds))
                {
                    // Se encontro
                    sPrecioCIVA = ds.Tables[0].Rows[0]["PrecioCIVA"].ToString();
                    bPromocion = "1";
                    // trar precio real
                    sQry = @" SELECT * FROM [ArticulosPrecios] WHERE (ArticulosID=" + sArticulosID + ") AND(SucursalesID=" + pSucursalesID + ") AND(Nivel='NV1')  ";
                    ds = null;
                    ds = qryToDataSet(sQry);
                    if (hayInfoDS(ds))
                    {
                        //precio resl
                        sPrecioRealCIVA = ds.Tables[0].Rows[0]["PrecioCIVA"].ToString();

                    }

                }
                else {
                    // se checa el precio para este articulo
                    sQry = @"SELECT 
	ArticulosPrecios.ArticulosPreciosID AS ArticulosPreciosID,	
	ArticulosPrecios.Precio AS Precio,	
	ArticulosPrecios.IVAPrecio AS IVAPrecio,	
	ArticulosPrecios.PrecioCIVA AS PrecioCIVA,	
	ArticulosPrecios.Costo AS Costo,	
	ArticulosPrecios.CostoCIVA AS CostoCIVA,	
	ArticulosPrecios.ArticulosID AS ArticulosID,	
	ArticulosPrecios.EsPromocion AS EsPromocion,	
	ArticulosPrecios.FechaInicio AS FechaInicio,	
	ArticulosPrecios.FechaFinal AS FechaFinal,	
	ArticulosPrecios.Nivel AS Nivel,	
	ArticulosPrecios.LimiteDeUnidades AS LimiteDeUnidades
FROM 
	ArticulosPrecios
WHERE 
	ArticulosPrecios.ArticulosID = "+sArticulosID+@"
	AND	ArticulosPrecios.SucursalesID = "+pSucursalesID+@"
	AND	ArticulosPrecios.EsPromocion = 0
		AND	ArticulosPrecios.Nivel ='NV1'";
                    ds = null;
                    ds = qryToDataSet(sQry);
                    if (hayInfoDS(ds))
                    {
                        sPrecioCIVA = ds.Tables[0].Rows[0]["PrecioCIVA"].ToString();
                        bPromocion = "0";
                        sPrecioRealCIVA = ds.Tables[0].Rows[0]["PrecioCIVA"].ToString();

                    }





                }
              
                return sArticulosID+"|"+sDescripcionArticulo+"|"+sPrecioCIVA+"|"+bPromocion+"|"+sPrecioRealCIVA;
            }


          


            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "PrecioVerificador:" + ex.Message + ex.StackTrace + "\n" + sQry);
                return "";
            }

            return "";
        }

        private Boolean hayInfoDS(System.Data.DataSet ds) {

            try {
                if (ds.Tables.Count > 0) {
                    if (ds.Tables[0].Rows.Count > 0) {

                        return true;
                    }


                }
                return false;

            } catch {

                return false;
            }

        }

        [WebMethod(Description = "Regresa el nombre de las sucursales con su DireccionIP correspodiente")]
        public string ObtenerSucursales()
        {
            String sQry = "select Nombre,DireccionIP,SucursalesID from Sucursales where Activo = 1 and EsAlmacen = 0";
            System.Data.DataSet ds;
            System.Xml.XmlElement xmlElement;
            try
            {
                ds = qryToDataSet(sQry);
                if(ds.Tables.Count>0)
                {
                    xmlElement = Serialize(ds.Tables[0]);
                    return xmlElement.OuterXml.ToString();
                }
                return "";
            }
            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "ObtenerSucursales:" + ex.Message + ex.StackTrace + "\n" + sQry);
                return "Ocurrio un error inesperado";
            }
        }


//---------------------------------[    Enrique     ]------------------------------------
        [WebMethod(Description = "Regresa solicitudes de cancelacion")]
                public string SolicitudCancelaciones(Boolean Activo, String FechaSolicitudIncio, String FechaSolicitudFinal, Int SucursalID)
                {
                    String sQry = @"
                SELECT 
                AutorizacionMovimientos.AutorizacionMovimientosID AS AutorizacionMovimientosID,	
                AutorizacionMovimientos.SucursalesID AS SucursalesID,	
                AutorizacionMovimientos.TiposMovimientosID AS TiposMovimientosID,	
                AutorizacionMovimientos.EmpleadoSolicitoID AS EmpleadoSolicitoID,	
                AutorizacionMovimientos.Justificacion AS Justificacion,	
                AutorizacionMovimientos.FechaHoraSolicitud AS FechaHoraSolicitud,	
                AutorizacionMovimientos.EsAprobado AS EsAprobado,	
                AutorizacionMovimientos.EsDenegado AS EsDenegado,	
                AutorizacionMovimientos.EmpleadoAutorizoID AS EmpleadoAutorizoID,	
                AutorizacionMovimientos.ComentariosAutorizacion AS ComentariosAutorizacion,	
                AutorizacionMovimientos.FechaHoraAutorizacion AS FechaHoraAutorizacion,	
                AutorizacionMovimientos.Activo AS Activo,	
                AutorizacionMovimientos.FolioMovimientoID AS FolioMovimientoID,	
                AutorizacionMovimientos.Procesado AS Procesado,	
                AutorizacionMovimientos.ProcesadoSolicitante AS ProcesadoSolicitante,	
                AutorizacionMovimientos.FechaProcesadoSolicitante AS FechaProcesadoSolicitante
                FROM 
                AutorizacionMovimientos
                WHERE 
                AutorizacionMovimientos.Procesado = "+Activo+@"
                AND	AutorizacionMovimientos.FechaHoraSolicitud BETWEEN '"+FechaSolicitudIncio+"' AND '"+FechaSolicitudFinal+@"'
                AND	AutorizacionMovimientos.SucursalesID = '"+SucursalID+"' ";
                  
                    System.Data.DataSet ds;
                    System.Xml.XmlElement xmlElement;
                    try
                    {
                        ds = qryToDataSet(sQry);
                        if(ds.Tables.Count>0)
                        {
                            xmlElement = Serialize(ds.Tables[0]);
                            return xmlElement.OuterXml.ToString();
                        }
                        return "";
                    }
                    catch (Exception ex)
                    {
                        System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "Regresa solicitudes de cancelacion:" + ex.Message + ex.StackTrace + "\n" + sQry);
                        return "Ocurrio un error inesperado";
                    }
                }
//----------------------------------------------------------------------------------    
    }
}
