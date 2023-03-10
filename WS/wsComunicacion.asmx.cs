using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Services;
using System.Drawing;
using System.Media;

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


                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "ExistenciaLocal:" + ex.Message + ex.StackTrace);
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


                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "qryToDataSet:" + ex.Message + "\n" + ex.StackTrace);
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

                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "qryInsertUpdate:" + ex.Message + ex.StackTrace);

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
 " and ArticulosPrecioMayoreo.Activo = 1  and PorcentajeMayoreo.Activo=1 and ArticulosPrecioMayoreo.PorcentajeMayoreoID =" + PorcentarID;






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
        [WebMethod(Description = "Regresa un xml del cliente enviado por codigo")]
        public String obtenerClientesPorCodigo(String sCodigo)
        {

            //    String sResultado = "";

            String query = "SELECT * FROM [Clientes] WHERE Codigo=" + sCodigo;



            System.Data.DataSet ds = qryToDataSet(query);


            System.Xml.XmlElement xmlElement = Serialize(ds.Tables[0]);

            return xmlElement.OuterXml.ToString();
        }
        [WebMethod(Description = "Regresa un xml del cliente enviado")]
        public String obtenerPedidos(String sClientesID, String sMes)
        {

            //    String sResultado = "";

            String query = "SELECT *,(SELECT count (PreVenta_Mayoreo_Detalle.ArticulosID) AS No from PreVenta_Mayoreo_Detalle where PreVenta_Mayoreo_Detalle.PreVenta_MayoreoID=PreVenta_Mayoreo.PreVenta_MayoreoID) as NoProductos FROM [PreVenta_Mayoreo] WHERE (Activo='1') and ClientesID=" + sClientesID + " and FechaHora BETWEEN '" + sMes + "01000001' and '" + sMes + "31235959' order by FechaHora";



            System.Data.DataSet ds = qryToDataSet(query);


            System.Xml.XmlElement xmlElement = Serialize(ds.Tables[0]);

            return xmlElement.OuterXml.ToString();
        }
        [WebMethod(Description = "Regresa un xml del cliente enviado")]
        public String obtenerPedidosDetalle(String sPedidosID)
        {

            //    String sResultado = "";

            String query = "SELECT PreVenta_Mayoreo_Detalle.ArticulosID,Articulos.Nombre as NombreArticulo,Articulos.Codigo,PreVenta_Mayoreo_Detalle.PrecioUCIVA_Cliente as PrecioCIVA,PreVenta_Mayoreo_Detalle.Cantidad,Articulos.PermitirDecimales,PreVenta_Mayoreo_Detalle.Importe,PreVenta_Mayoreo_Detalle.Notas " +
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
        public int HaddPedido(String sPedido, String sDetalle)
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
                if (arr.Length > 11)
                {
                    qry = "Insert into PreVenta_Mayoreo (FechaHora,ClientesID,PorcentajeMayoreoID,NombreCliente,Subtotal,IVA,Importe,EmpleadosID,Notas,FechaEntrega,Activo,Facturacion,Tipo_Cambio,ImporteDolares) " +
                                                   " Values ('" + arr[0] + "'," + arr[1] + "," + arr[2] + ",'" + arr[3] + "'," + arr[4] + "," + arr[5] + "," + arr[6] + "," + arr[7] + ",'" + arr[8] + "','" + arr[9] + "',1," + arr[10] + "," + arr[11] + "," + arr[12] + ")";

                }
                else
                {
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

        [WebMethod(Description = "registra detalle pedido  // ArticulosID|Cantidad|IVA|Importe|PrecioUCIVA_Cliente|PrecioUSIVA_Cliente|CostoCIVA|CostoSIVA|PrecioCIVA|PrecioSIVA|Notas" + " @ seperaer un detale de otro")]

        public String HaddPedidoDetalle(String sPedidoDetalle, String PreVenta_MayoreoID)
        {
            //   Boolean bBandera = false;

            String[] arrDetalle;

            String sRespuesta = "";
            String qry = "";

            String preciosIVa_Original = "0";
            String precioCIVA_Original = "0";
            String precioSiVA = "0";
            String precioCIVA = "0";
            try
            {

                arrDetalle = sPedidoDetalle.Split('@');


                foreach (String s in arrDetalle)
                {
                    if (s.Length > 3)
                    {

                        String[] arr;
                        arr = s.Split('|');




                        if (arr[8] == "")
                        {
                            precioCIVA_Original = "0";
                        }
                        else
                        {
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
                                                           " Values (" + arr[0] + "," + arr[1] + "," + arr[2] + "," + arr[3] + "," + precioCIVA + "," + precioSiVA + "," + arr[6] + "," + arr[7] + "," + precioCIVA_Original + "," + preciosIVa_Original + "," + PreVenta_MayoreoID + ",'" + arr[10] + "')";

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


                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "PreVenta_MayoreoID:" + PreVenta_MayoreoID + "/n" + ex.Message);


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
        public System.Drawing.Image byteArrayToImage(byte[] byteArrayIn)
        {
            System.Drawing.Image returnImage = null;


            try
            {
                System.IO.MemoryStream ms = new System.IO.MemoryStream(byteArrayIn, 0, byteArrayIn.Length);
                ms.Write(byteArrayIn, 0, byteArrayIn.Length);
                returnImage = System.Drawing.Image.FromStream(ms, true);//Exception occurs here
            }
            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "byteArraytoImage:/n" + ex.Message + ex.StackTrace);

            }
            return returnImage;
        }
        [WebMethod]
        public Boolean guardarFotoCliente(byte[] f, String sClienteID)
        {

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

                String qry = "Update clientes set fotoLocal=? where ClientesID = " + sClienteID;



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
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "Imagen ClienteID:" + sClienteID + "/n" + ex.Message + ex.StackTrace);

                transaction.Rollback();
            }
            finally
            {

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
                ds.Tables[0].TableName = "MayoreoDetalle";

                ds.WriteXml(nombre + ".xml");


                reporte.RegData("MayoreoDetalle", "MayoreoDetalle", ds);
                //  reporte.Dictionary.Synchronize();


                reporte["id"] = PreVenta_MayoreoID;



                reporte.Compile();

                reporte.Render();

                reporte.ExportDocument(Stimulsoft.Report.StiExportFormat.Pdf, nombre + ".pdf");


                return true;


            }
            catch (Exception ex)
            {


                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "PdfPedido:" + ex.Message + "\n" + ex.StackTrace);

            }

            //   return    byte[] todecode_byte = Convert.FromBase64String(data); // data es el string en BASE64
            return false;
        }
        [WebMethod(Description = "get pdf")]
        public String GetPedidoPDF(String NoPedido)
        {
            try
            {

                byte[] bytes = System.IO.File.ReadAllBytes(@"C:\sXML\PED_" + NoPedido + ".pdf");
                return Convert.ToBase64String(bytes);
            }
            catch (Exception ex)
            {



                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", ex.Message);

            }
            return "";
        }
        [WebMethod(Description = "EnviarPor corroe pdf")]
        public Boolean EnviarPedidoPDF(String NoPedido, String sDestino)
        {

            String correode = "info@fruteriasnenas.com";
            String clavecorreo = "N1e2n3a4s";

            // Replace recipient@example.com with a "To" address. If your account 
            // is still in the sandbox, this address must be verified.
            String TO = sDestino;


            // The subject line of the email
            String SUBJECT =
                "Envio de Preventa :" + NoPedido;

            // The body of the email
            String BODY =
                "<h1>Su Preventa se ha registrado Correctamente</h1>" +
                "<p>Se Anexa su comprobante de Preventa.</p>";

            // Create and build a new MailMessage object
            System.Net.Mail.MailMessage message = new System.Net.Mail.MailMessage();
            message.IsBodyHtml = true;
            message.From = new System.Net.Mail.MailAddress(correode, "Fruteria Nenas");
            message.To.Add(new System.Net.Mail.MailAddress(TO));
            message.Subject = SUBJECT;
            message.Attachments.Add(new System.Net.Mail.Attachment(@"C:\sXML\PED_" + NoPedido + ".pdf"));
            message.Body = BODY;
            // Comment or delete the next line if you are not using a configuration set
            // message.Headers.Add("X-SES-CONFIGURATION-SET", CONFIGSET);

            using (System.Net.Mail.SmtpClient smtp = new System.Net.Mail.SmtpClient())
            {
                try
                {
                    smtp.Host = "smtp.ipage.com";
                    smtp.Port = 587;
                    smtp.EnableSsl = false;
                    System.Net.NetworkCredential NetworkCred = new System.Net.NetworkCredential();
                    NetworkCred.UserName = correode;
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
        public String validarLoguin(String sUsuario, String sPassword)
        {

            try
            {
                String q = "SELECT Usuarios.UsuariosID,Empleados.EmpleadosID , Empleados.Codigo, Empleados.NombreCompleto from Empleados,Usuarios where Empleados.EmpleadosID=Usuarios.EmpleadosID and Usuarios.ConreasenaAPP='" + sPassword + "' and Usuarios.Nombre='" + sUsuario + "' and Usuarios.Activo=1 and empleados.Baja_temporal=0 and Empleados.Activo=1 ";


                System.Data.DataSet ds = qryToDataSet(q);


                if (ds.Tables.Count > 0)
                {


                    System.Xml.XmlElement xmlElement = Serialize(ds.Tables[0]);

                    return xmlElement.OuterXml.ToString();

                }

            }
            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "validarLoguin:" + ex.Message + ex.StackTrace);

            }

            return "";
        }
        [WebMethod(Description = "Actualiar contraseña")]
        public Boolean actualiarPassword(String nUsuarioID, String sNewPassword)
        {

            try
            {
                String q = "Update Usuarios set Usuarios.ConreasenaAPP='" + sNewPassword + "' Usuarios.UsuariosID=" + nUsuarioID + "  ";

                return qryInsertUpdate(q);


            }
            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", ex.Message);

            }

            return false;
        }

        [WebMethod(Description = "Regresa un xml con las existencias configuradas")]
        public String obtenerExistenciaMayoreo()
        {

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
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "obtenerExistenciaMayoreo:" + ex.Message + ex.StackTrace);

            }

            return "";
        }
        [WebMethod(Description = "Regresa un xml la informacion de pedidos")]
        public String obtenerPedidoAlmacen(String sPedidoID)
        {

            try
            {
                String q = "SELECT * from pedidos where pedidosID=" + sPedidoID;


                System.Data.DataSet ds = qryToDataSet(q);


                if (ds.Tables.Count > 0)
                {


                    System.Xml.XmlElement xmlElement = Serialize(ds.Tables[0]);

                    return xmlElement.OuterXml.ToString();

                }

            }
            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "obtenerPedidoAlmacen:" + ex.Message + ex.StackTrace);

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
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "obtenerDetallePedidoAlmacen:" + ex.Message + ex.StackTrace);

            }

            return "";
        }

        [WebMethod(Description = "Regresa un xml la informacion de NOpedidos")]
        public String obtenerInfoEstatisticas(String sEmpleadoID, String sMes)
        {

            try
            {
                String q = "SELECT 'NoPreventas' as Tipo,count(EmpleadosID) as Cantidad from PreVenta_Mayoreo where EmpleadosID=" + sEmpleadoID + " and Activo=1 and  FechaHora BETWEEN '" + sMes + "01000001' and '" + sMes + "31235959'";
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
                    if (ds2.Tables[0].Rows.Count > 0)
                    {
                        System.Data.DataRow r = ds.Tables[0].NewRow();

                        r["Tipo"] = "+" + ds2.Tables[0].Rows[0]["Nombre"].ToString();
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
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "obtenerInfoEstatisticas:" + ex.Message + ex.StackTrace);

            }

            return "";
        }
        //
        [WebMethod(Description = "Regresa el valor del tipo de cambio actual Compras")]
        public String obtenerTCCompras(String sSucursalID)
        {

            try
            {
                String q = "SELECT TCVenta from DatosSucursal where SucursalesID=" + sSucursalID;


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





            return "1.0.0.20";

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
                String q = "SELECT Codigo from Clientes where RFC='" + sRFC + "'";


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
                qry += ",24";
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



                qry += "," + siguienteFolio("Clientes");
                qry += ",'" + arr[24] + "'";
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

            if (bBandera)
            {
                return "1";
            }
            else
            {
                return "";
            }




        }

        private int siguienteFolio(String sArchivo)
        {

            String q = "SELECT * from ConsecutivoFolio where Archivo='" + sArchivo + "'";
            int nConsecutivo = 1;
            int id = 0;
            System.Data.DataSet ds = qryToDataSet(q);


            if (ds.Tables.Count > 0)
            {
                id = int.Parse(ds.Tables[0].Rows[0]["ConsecutivoFolioID"].ToString());
                nConsecutivo = int.Parse(ds.Tables[0].Rows[0]["Consecutivo"].ToString());
                nConsecutivo += 1;

                q = "Update ConsecutivoFolio set Consecutivo=" + nConsecutivo + " where ConsecutivoFolioID=" + id;
                //Actualizar
                Boolean bBandera = qryInsertUpdate(q);

                nConsecutivo = nConsecutivo * 1000;
                //id de la sucurdal
                nConsecutivo += 24;


                if (bBandera)
                {


                }
                else
                {
                    nConsecutivo = -1;
                }

            }


            return nConsecutivo;
        }

        [WebMethod(Description = "Guarda la posicion del dispositivo")]
        public Boolean HAddPosicion(String sEmpleadoID, String latr, String lon, String sAPP, String sIMEI, String sNota, String Fhora)
        {

            // se tien que crear una tabla  para k
            //almacenara el registro
            //LogPosicionesGPS
            //LogPosicionesGPSID
            //EmpleadoID
            //Latitud
            //Logitud
            //Dispositivo
            //Notas
            Boolean bBandera = false;
            String q = "Insert Into LogGPS (APP,FechaHora,Latitud,Longitud,IMEI,EmpleadosID,Notas) values (" +
                "'" + sAPP + "'," +
                  "'" + Fhora + "'," +
                  "'" + latr + "'," +
                   "'" + lon + "'," +
                    "'" + sIMEI + "'," +
                     "" + sEmpleadoID + "," +
                      "'" + sNota + "'"
                + ")";
            //   System.IO.File.WriteAllText(@"C:\sXML\HAddPosicion_" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", q);


            try
            {
                bBandera = false;

                bBandera = qryInsertUpdate(q);


            }
            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\HAddPosicion_" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", ex.Message + "\n" + ex.StackTrace);
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
        public String obtenerInfoReplicarcion(String sTabla, String sID)
        {

            try
            {
                String q = "SELECT * from " + sTabla + " where " + sTabla + "ID in (" + sID + ")";


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
        public Boolean articulosActivosAPP(String sArticulosID, String sSucursalesID, String bElimina)
        {


            Boolean bBandera = false;
            String q = "Delete from AppArticulosDisponibilidad ";
            String[] arr;
            arr = sArticulosID.Split('|');
            //Eliminar registro de la sucursal





            try
            {
                bBandera = false;
                if (bElimina == "1")
                {

                    bBandera = qryInsertUpdate(q);
                }
                else
                {
                    bBandera = true;
                }



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
        public String obtenerHistorialEntradasPedidos(String SucursalesID, String sFini, String sFFin, String sArticuloID)
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
		Pedidos.SucursalesID = " + SucursalesID + @"
		AND Pedidos.Estatus_PedidosID = 3 
        AND Pedidos.FechaRecibido BETWEEN '" + sFini + @"'
         AND '" + sFFin + @"'

	";


                if (sArticuloID.Length > 2)
                {

                    q += @"  and Pedidos.PedidosID in (SELECT Pedidos.PedidosID from pedidos,Pedidos_Articulos 
                    where Pedidos.PedidosID = Pedidos_Articulos.PedidosID
                    and Pedidos.FechaRecibido BETWEEN '" + sFini + @"'  and '" + sFFin + @"'
                    and Pedidos.Estatus_PedidosID = 3 and Pedidos.SucursalesID = " + SucursalesID + @" and Pedidos_Articulos.ArticulosID = " + sArticuloID + ")";


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
        public String obtenerHistorialEntradas(String SucursalesID, String sFini, String sFFin, String sArticuloID, String sTipoMovimientoID, String sRecepcionLocal, String sDolares, String sEstatus, String sFamiliasID, String sLineasID, String sFolioMov, String sEntradasID, String sReferencia, String sProveedorID, String sOrigenID, String sTieneBonificado)
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
	                    Entradas.SucursalesID =" + SucursalesID + @"

                    AND	Entradas.FechaEntrada BETWEEN '" + sFini + @"' AND '" + sFFin + @"' ";


                if (sTipoMovimientoID.Length > 1)
                {

                    q += "  AND	Entradas.TiposMovimientosID =" + sTipoMovimientoID;

                }

                if (sRecepcionLocal.Length > 1)
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

                    q += " AND	Entradas.Referencia  = '" + sReferencia + @"'";

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
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "obtenerHistorialEntradasPedidos:" + ex.Message + ex.StackTrace + "\n" + q);

            }

            return "";
        }
        [WebMethod(Description = "Regresa un xml la informacion de Tabla")]
        public String obtenerArticulosEntradas(String sEntradasID)
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
		                EntradasArticulos.EntradasID =" + sEntradasID + ")";




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
		            EntradasArticulos.EntradasID = " + sPedidosID + ")";




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
		            AND	Ventas.FechaVenta ='" + sFecha + @"'
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
        public Boolean UpdateVentasMRY(String sFecha)
        {

            Boolean bBandera = false;
            String q = @"UPDATE 
	                    Ventas
                    SET
	                    IPInventarioAfecto =''
                    WHERE 
	                    Ventas.IPInventarioAfecto ='192.168.10.253'
	                     AND Ventas.FechaVenta=" + sFecha;
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

                if (System.IO.Directory.Exists("C:/sXML/importacion /") == false)
                {
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
        public String Qry_INV_PedidosHistoriales(string pSucursalesID, string pFechaRInicio, string pFechaRFinal, string pFechaSInicio, string pFechaSFinal, string pSucursalAlmacenID, string pEstatus_PedidosID)
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

                if (pFechaRInicio.Length > 0 && pFechaRFinal.Length > 0)
                {

                    q += " AND Pedidos.FechaRecibido BETWEEN '" + pFechaRInicio + "' AND '" + pFechaRFinal + "' ";
                }


                if (pSucursalesID.Length > 0)
                {

                    q += " AND Pedidos.SucursalesID = " + pSucursalesID;
                }

                if (pSucursalAlmacenID.Length > 0)
                {


                    q += " AND Pedidos.SucursalAlmacenID = " + pSucursalAlmacenID;
                }

                if (pEstatus_PedidosID.Length > 0)
                {
                    q += "  AND Pedidos.Estatus_PedidosID IN (" + pEstatus_PedidosID + ") ";
                }




                q += " )";

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
        public String QRY_Salidas_X_ArticuloID_S_Fecha(string pArticulosID, string pFechaInicial, string pFechaFInal, string pDestinosID, string pTipoMovimientoID, string pSalidasID, string pSucursalesID, string pfolioMovimiento, string pEstatusMovimientoID, string pFamiliasID, string pLineasID, string pProveedoresID, string pPendienteAplicarNC)
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
	                    Salidas.FechaSalida BETWEEN '" + pFechaInicial + @"' AND '" + pFechaFInal + @"'
                    AND	Salidas.SucursalesID =" + pSucursalesID;


                if (pArticulosID.Length > 0)
                {
                    q += " AND	SalidasArticulos.ArticulosID =  " + pArticulosID;
                }

                if (pDestinosID.Length > 0)
                {

                    q += " AND	Salidas.DestinosID =" + pDestinosID;
                }


                if (pTipoMovimientoID.Length > 0)
                {

                    q += " AND	Salidas.TiposMovimientosID =" + pTipoMovimientoID;
                }

                if (pSalidasID.Length > 0)
                {

                    q += "AND	Salidas.SalidasID =" + pSalidasID;
                }
                if (pfolioMovimiento.Length > 0)
                {

                    q += "AND	Salidas.FolioMovimiento =" + pfolioMovimiento;
                }

                if (pEstatusMovimientoID.Length > 0)
                {

                    q += " AND	Salidas.Estatus_MovimientosID =" + pEstatusMovimientoID;
                }
                if (pFamiliasID.Length > 0)
                {
                    q += " AND	Articulos.FamiliasID = " + pFamiliasID;
                }

                if (pLineasID.Length > 0)
                {

                    q += " AND	Articulos.LineaID =" + pLineasID;
                }
                if (pProveedoresID.Length > 0)
                {
                    q += "	AND	Salidas.ProveedoresID = " + pProveedoresID;
                }

                if (pPendienteAplicarNC.Length > 0)
                {

                    q += " AND	Salidas.PendienteAplicarNC =" + pPendienteAplicarNC;
                }


                q += " ) ORDER BY  FechaSalida ASC";


                System.Data.DataSet ds = qryToDataSet(q);


                if (ds.Tables.Count > 0)
                {


                    System.Xml.XmlElement xmlElement = Serialize(ds.Tables[0]);

                    return xmlElement.OuterXml.ToString();

                }

            }
            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "QRY_Salidas_X_ArticuloID_S_Fecha:" + ex.Message + ex.StackTrace + ex.InnerException + "\n" + q);

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
			                    Salidas.SucursalesID =" + pSucursalesID + @"
		                    AND	SalidasArticulos.SalidasID IN (" + pSalidasID + @")
		
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
        public String obtenerHistorialEntradasPedidosDetalle(String pInPedidosID)
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
                                AND Pedidos.PedidosID in (" + pInPedidosID + @")
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
            String sQry = "";
            try
            {
                sQry = @"SELECT Empleados.PuestosID, Puestos.Nombre AS nombrePuesto, Empleados.DepartamentosID, Departamentos.Descripcion AS nombreDepartamento, Empleados.Activo,
                        Empleados.Nombre, Empleados.ApellidoPaterno, Empleados.ApellidoMaterno, Empleados.sexo, Empleados.FechaIngreso, Empleados.FechaNacimiento, Empleados.EstadoCivil, Empleados.NivelEstudios,
                        Historial_Bajas.Fecha_Baja, Historial_Bajas.Comentarios, Historial_Bajas.EmpleadosID,Historial_Bajas.MotivosDeBajaCatalogo 
                        FROM Historial_Bajas JOIN Empleados on Empleados.EmpleadosID = Historial_Bajas.EmpleadosID 
                        JOIN Puestos ON Empleados.PuestosID = Puestos.PuestosID 
                        JOIN Departamentos ON Empleados.DepartamentosID = Departamentos.DepartamentosID WHERE Empleados.SucursalesID = " + pSucursalesID + @" AND 
                         Historial_Bajas.Fecha_Baja BETWEEN '" + FechaInicio + @"' AND '" + FechaFinal + "' ORDER BY Fecha_Baja";


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
                BETWEEN '" + FechaInicio + @"' AND '" + FechaFinal + "' AND Empleados.SucursalesID = " + pSucursalesID;

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
             FROM Puestos_Sucursales WHERE Puestos_Sucursales.Activo = 1 AND Puestos_Sucursales.SucursalesID = " + pSucursalesID;

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
        public String PrecioVerificador(String pSucursalesID, String sCodigo)
        {
            System.Xml.XmlElement xmlElement;
            String sQry = "";
            String sArticulosID = "";
            String sDescripcionArticulo = "";
            String sPrecioCIVA = "";
            String bPromocion = "";
            String sPrecioRealCIVA = "";
            String sPermitirDecimales = "";
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
	                    Articulos.Codigo =" + "'" + sCodigo + "'";


                System.Data.DataSet ds = qryToDataSet(sQry);
                if (hayInfoDS(ds))
                {
                    // Se encontro
                    sArticulosID = ds.Tables[0].Rows[0]["ArticulosID"].ToString();
                    sDescripcionArticulo = ds.Tables[0].Rows[0]["Nombre"].ToString();
                    sPermitirDecimales = ds.Tables[0].Rows[0]["PermitirDecimales"].ToString();

                }
                else
                {
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
		                        CodigosBarra_Articulos.CodigoBarra =" + "'" + sCodigo + "'" + ")";
                    ds = null;
                    ds = qryToDataSet(sQry);



                    if (hayInfoDS(ds))
                    {

                        // Se encontro
                        sArticulosID = ds.Tables[0].Rows[0]["ArticulosID"].ToString();
                        sDescripcionArticulo = ds.Tables[0].Rows[0]["Nombre"].ToString();
                        sPermitirDecimales = ds.Tables[0].Rows[0]["PermitirDecimales"].ToString();


                    }
                    else
                    {
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
	                    ArticulosPrecios.ArticulosID = " + sArticulosID + @"
	                    AND	ArticulosPrecios.SucursalesID =" + pSucursalesID + @"
	                    AND	ArticulosPrecios.EsPromocion = 1
	                    AND	ArticulosPrecios.FechaInicio <= '" + DateTime.Now.ToString("yyyyMMdd") + @"'
	                    AND	ArticulosPrecios.FechaFinal >= '" + DateTime.Now.ToString("yyyyMMdd") + @"'
	                    AND	ArticulosPrecios.Activo =1";
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
                else
                {
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
	                        ArticulosPrecios.ArticulosID = " + sArticulosID + @"
	                        AND	ArticulosPrecios.SucursalesID = " + pSucursalesID + @"
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

                return sArticulosID + "|" + sDescripcionArticulo + "|" + sPrecioCIVA + "|" + bPromocion + "|" + sPrecioRealCIVA + "|" + sPermitirDecimales;
            }





            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "PrecioVerificador:" + ex.Message + ex.StackTrace + "\n" + sQry);
                return "";
            }

            return "";
        }

        private Boolean hayInfoDS(System.Data.DataSet ds)
        {

            try
            {
                if (ds.Tables.Count > 0)
                {
                    if (ds.Tables[0].Rows.Count > 0)
                    {

                        return true;
                    }


                }
                return false;

            }
            catch
            {

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
                if (ds.Tables.Count > 0)
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


        [WebMethod(Description = "Plantilla por puestos")]
        public String ObtenerPuestosPlantilla(String pSucursalesID)
        {
            System.Xml.XmlElement xmlElement;
            String sQry = "";
            try
            {
                sQry = @"SELECT Puestos_Sucursales.PuestosID, Puestos_Sucursales.CantidadEmpleados
             FROM Puestos_Sucursales WHERE Puestos_Sucursales.Activo = 1 AND Puestos_Sucursales.SucursalesID = " + pSucursalesID;

                System.Data.DataSet ds = qryToDataSet(sQry);
                if (ds.Tables.Count > 0)
                {
                    xmlElement = Serialize(ds.Tables[0]);
                    return xmlElement.OuterXml.ToString();
                }
            }

            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "Obtener Puesto Plantilla:" + ex.Message + ex.StackTrace + "\n" + sQry);

            }

            return "";
        }


        //---------------------------------[    Solicitudes de Cancelacion   ]------------------------------------
        [WebMethod(Description = "Regresa solicitudes de cancelacion")]
        public string SolicitudCancelaciones(String Activo, String FechaSolicitudIncio, String FechaSolicitudFinal, String SucursalID)
        {
            String sQry = @"
                SELECT *
                FROM 
                AutorizacionMovimientos
                WHERE 
                AutorizacionMovimientos.Procesado = " + Activo + @"
                AND	AutorizacionMovimientos.FechaHoraSolicitud BETWEEN '" + FechaSolicitudIncio + "' AND '" + FechaSolicitudFinal + @"'
                AND	AutorizacionMovimientos.SucursalesID = " + SucursalID;


            System.Data.DataSet ds;
            System.Xml.XmlElement xmlElement;
            try
            {
                ds = qryToDataSet(sQry);
                if (ds.Tables.Count > 0)
                {
                    xmlElement = Serialize(ds.Tables[0]);
                    return xmlElement.OuterXml.ToString();
                }
                return "No ingreso en el if ";
            }
            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "Regresa solicitudes de cancelacion:" + ex.Message + ex.StackTrace + "\n" + sQry);
                return "Ocurrio un error inesperado";
            }
        }



        [WebMethod(Description = "Modifica la tabla AutorizacionMovientos")]
        public string Autorizacion_Desicion(String MovimientoID, String Desicion, String JustificacionDesicion, String FechaAutorizacion, String IDEmpleadoAutorizo)
        {
            String sQry = "";
            //Construimos el qry 
            try
            {
                sQry = @"UPDATE AutorizacionMovimientos
                        SET ComentariosAutorizacion='" + JustificacionDesicion + "', FechaHoraAutorizacion='" + FechaAutorizacion + "', EmpleadoAutorizoID='" + IDEmpleadoAutorizo + "', Procesado=1";

                //Marcamos la casilla de EsAprobado
                if (Desicion == "1")
                {
                    sQry += ", EsAprobado=1";
                }
                else
                {
                    //Marcamos la casilla de EsRechazado
                    sQry += ", EsDenegado=1";
                }
                //Instruccion final del qry
                sQry += " WHERE FolioMovimientoID=" + MovimientoID;

                //ejecutamos el qry
                qryInsertUpdate(sQry);
            }
            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", ex.Message);
            }
            return sQry;
        }



        [WebMethod(Description = "Regresa historial de sincronizaciones sistema de emergencia")]
        public string SistemaEmergenciaHistorial(String FechaSolicitudIncio, String FechaSolicitudFinal, String SucursalID)
        {
            String sQry = @"
                SELECT *
                FROM 
                LogTiposMovimientosDesHora
                WHERE 
                LogTiposMovimientosDesHora.FechaHoraCreacion BETWEEN '" + FechaSolicitudIncio + "' AND '" + FechaSolicitudFinal + @"'
                AND	LogTiposMovimientosDesHora.SucursalesID = " + SucursalID;

            System.Data.DataSet ds = new System.Data.DataSet();
            System.Xml.XmlElement xmlElement;
            try
            {
                ds = qryToDataSet(sQry);
                if (ds.Tables.Count > 0)
                {
                    xmlElement = Serialize(ds.Tables[0]);
                    return xmlElement.OuterXml.ToString();
                }
                return "No ingreso en el if ";
            }
            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "Sistema emergencia historial:" + ex.Message + ex.StackTrace + "\n" + sQry + "\n");
                return "Ocurrio un error inesperado";
            }
        }

        [WebMethod(Description = "Agrega justificacion a los registros que se quedan en el limbo")]
        public string Historial_Cancelacion_Justificacion_Limbo(String MovimientoID, String JustificacionDelLimbo)
        {
            String sQry = "";
            //Construimos el qry 
            try
            {
                sQry = @"UPDATE AutorizacionMovimientos
                        SET ComentariosAutorizacion='" + JustificacionDelLimbo + "', Justificacion_No_Cancelada=1";

                //Instruccion final del qry
                sQry += " WHERE FolioMovimientoID=" + MovimientoID;

                //ejecutamos el qry
                qryInsertUpdate(sQry);
            }
            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", ex.Message);
            }
            return sQry;
        }
        //----------------------------------------------------------------------------------    


        [WebMethod(Description = "Regresa el Id de las subcategorias asi como su nombre")]
        public string ObtenerSubCategorias(string SubCategoriasID)
        {
            String sQry = "select AppSubCategoriasID,Descripcion from AppSubCategorias where AppSubCategoriasID in (" + SubCategoriasID + ")";
            System.Data.DataSet ds;
            System.Xml.XmlElement xmlElement;
            try
            {
                ds = qryToDataSet(sQry);
                if (ds.Tables.Count > 0)
                {
                    xmlElement = Serialize(ds.Tables[0]);
                    return xmlElement.OuterXml.ToString();
                }
                return "";
            }
            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "ObtenerSubCategorias:" + ex.Message + ex.StackTrace + "\n" + sQry);
                return "Ocurrio un error inesperado";
            }

        }
        [WebMethod(Description = "Regresa Nombre y codigo de los articulos pertenecientes a esa categoria")]
        public string ObtenerArticulosDeSubcategoria(string SubcategoriaID)
        {
            String sQry = "select ArticulosID,Codigo,Nombre from Articulos where AppSubCategoriasID = " + SubcategoriaID + " order by Nombre asc";
            System.Data.DataSet ds;
            System.Xml.XmlElement xmlElement;
            try
            {
                ds = qryToDataSet(sQry);
                if (ds.Tables.Count > 0)
                {
                    xmlElement = Serialize(ds.Tables[0]);
                    return xmlElement.OuterXml.ToString();
                }
                return "";
            }
            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "ObtenerArticulosDeSubcategoria:" + ex.Message + ex.StackTrace + "\n" + sQry);
                return "Ocurrio un error inesperado";
            }
        }

        [WebMethod(Description = "Regresa los articulos que no se han contabilizado")]
        public string consultaArticulosNoContabilizados(String ParamFechaInicial, String ParamFechaFinal, String Param_ID_Sucursal, String Param_Articulo_Estatus, String Param_Linea_Producto, String Param_Tipo_De_Ciclico)
        {
            String sQry = @"SELECT Articulos.Codigo,Articulos.Nombre,Articulos.ArticulosID,ArticulosExistencias.Existencia,'19990101' as Fecha FROM Articulos,ArticulosExistencias
                          WHERE 
                          Articulos.ArticulosID=ArticulosExistencias.ArticulosID
                          AND ArticulosExistencias.SucursalesID=" + Param_ID_Sucursal + @"
                          AND Articulos.Activo=" + Param_Articulo_Estatus + @"
                          AND Articulos.SeVende=1 ";

            if (Param_Linea_Producto.Length > 0)
            {

                sQry += " AND Articulos.LineaID=" + Param_Linea_Producto;
            }

            sQry += @" AND Articulos.ArticulosID NOT IN (

                         SELECT DISTINCT(ConteosArticulos. ArticulosID) AS ArticulosID
                         FROM Conteos,ConteosArticulos
                         WHERE
                         Conteos.ConteosID=ConteosArticulos.ConteosID
                         AND Conteos.Fecha BETWEEN '" + ParamFechaInicial + @"' AND '" + ParamFechaFinal + @"'
                         AND Conteos.SucursalesID=" + Param_ID_Sucursal + @"
                         AND Conteos.EsBorrador=" + Param_Tipo_De_Ciclico + @"
                        )
                         ORDER BY 
                         Nombre ASC";



            System.Data.DataSet ds;
            System.Data.DataSet dsAux;
            System.Xml.XmlElement xmlElement;
            try
            {
                ds = qryToDataSet(sQry);

                String sArticulosID = "";

                foreach (System.Data.DataRow nRow in ds.Tables[0].Rows)
                {

                    if (nRow["ArticulosID"].ToString().Length > 1)
                    {

                        sArticulosID += nRow["ArticulosID"].ToString() + ",";
                    }


                }

                sArticulosID += "99";


                sQry = @"SELECT DISTINCT (ConteosArticulos.ArticulosID), MAX(Conteos.Fecha) AS UltimaFecha
   FROM
  Conteos, ConteosArticulos
  WHERE
  Conteos.ConteosID = ConteosArticulos.ConteosID
   AND Conteos.SucursalesID =" + Param_ID_Sucursal + @"
                AND ConteosArticulos.ArticulosID IN (" + sArticulosID + @" )
                GROUP BY ConteosArticulos.ArticulosID";


                dsAux = qryToDataSet(sQry);
                System.Data.DataRow[] result = null;

                foreach (System.Data.DataRow nRow in dsAux.Tables[0].Rows)
                {

                    result = ds.Tables[0].Select("ArticulosID =" + nRow["ArticulosID"].ToString());
                    if (result.Length > 0)
                    {
                        int SelectedIndex = ds.Tables[0].Rows.IndexOf(result[0]);

                        if (SelectedIndex > -1)
                        {
                            ds.Tables[0].Rows[SelectedIndex]["Fecha"] = nRow["UltimaFecha"];
                        }

                    }

                }



                if (ds.Tables.Count > 0)
                {
                    xmlElement = Serialize(ds.Tables[0]);
                    return xmlElement.OuterXml.ToString();
                }
                return "No ingreso en el if. ";
            }
            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "consultaArticulosNoContabilizados:" + ex.Message + ex.StackTrace + "\n" + sQry);
                return "Ocurrio un error inesperado";
            }
        }

        //-----------------------[Solicitudes Mermas]----------------------------
        [WebMethod(Description = "Regresa solicitudes de aprobacion Mermas")]
        public string SolicitudAprobacionMermas(String Activo, String FechaSolicitudIncio, String FechaSolicitudFinal, String SucursalID, String LineaID)
        {
            String sQry = @"
                SELECT *
                FROM 
                AutorizacionMovimientosMermas
                WHERE 
                AutorizacionMovimientosMermas.ProcesadoDireccion = " + Activo;

            if (LineaID != "-1")
            {
                sQry += " AND AutorizacionMovimientosMermas.LineaID = " + LineaID;
            }

            sQry += " AND	AutorizacionMovimientosMermas.FechaHoraSolicitud BETWEEN '" + FechaSolicitudIncio + "' AND '" + FechaSolicitudFinal + @"'
                AND	AutorizacionMovimientosMermas.SucursaID = " + SucursalID;

            System.Data.DataSet ds;
            System.Xml.XmlElement xmlElement;
            try
            {
                ds = qryToDataSet(sQry);
                if (ds.Tables.Count > 0)
                {
                    xmlElement = Serialize(ds.Tables[0]);
                    return xmlElement.OuterXml.ToString();
                }
                return "Nada para regresar";
            }
            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "Regresa solicitudes de cancelacion:" + ex.Message + ex.StackTrace + "\n" + sQry);
                return "Ocurrio un error inesperado";
            }
        }
        [WebMethod(Description = "Modifica la tabla AutorizacionMovientosMermas")]
        public string Autorizacion_Direccion_Mermas(String MovimientoID, String Desicion, String JustificacionDesicion, String FechaAutorizacion, String IDEmpleadoAutorizo)
        {
            String sQry = "";
            //Construimos el qry 
            try
            {
                sQry = @"UPDATE AutorizacionMovimientosMermas
                                 SET ComentariosAutorizacion='" + JustificacionDesicion + "', FechaHoraAutorizacion='" + FechaAutorizacion + "', EmpleadoAutorizoID='" + IDEmpleadoAutorizo + "', ProcesadoDireccion=1";

                //Marcamos la casilla de EsAprobado
                if (Desicion == "1")
                {
                    sQry += ", EsAprobado=1";
                }
                else
                {
                    //Marcamos la casilla de EsRechazado
                    sQry += ", EsDenegado=1";
                }
                //Instruccion final del qry
                sQry += " WHERE AutorizacionMovimientosMermasID=" + MovimientoID;
                //ejecutamos el qry
                qryInsertUpdate(sQry);
            }
            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", ex.Message);
            }
            return sQry;
        }

        //    }


        [WebMethod(Description = "Agrega justificacion a los registros que se quedan en el limbo en mermas")]
        public string Justificacion_Limbo_Mermas(String MovimientoID, String JustificacionDelLimbo)
        {
            String sQry = "";
            //Construimos el qry 
            try
            {
                sQry = @"UPDATE AutorizacionMovimientosMermas
                        SET ComentariosAutorizacion='" + JustificacionDelLimbo + "', EsSolicitudNoInteractuada=1";

                //Instruccion final del qry
                sQry += " WHERE AutorizacionMovimientosMermasID=" + MovimientoID;

                //ejecutamos el qry
                qryInsertUpdate(sQry);
            }
            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", ex.Message);
            }
            return sQry;
        }

        [WebMethod(Description = "Obtener Preventa Especifica")]
        public string ObtenerPreventaEspecifica(String PreventaID)
        {
            System.Xml.XmlElement xmlElement;
            try
            {
                String query = @"
                    Select 
                        FechaHora,
                        NombreCliente,
                        Subtotal,
                        importe,
                        EnDolares,
                        ClientesID,
                        Notas,
                        Activo,
                        esAutorizado,
                        IsProcesado,
                        Tipo_Cambio,
                        credito
                    From 
                        PreVenta_Mayoreo 
                    Where 
                        PreVenta_MayoreoID = " + PreventaID;
                System.Data.DataSet ds = qryToDataSet(query);
                xmlElement = Serialize(ds.Tables[0]);
                return xmlElement.OuterXml.ToString();
            }
            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", ex.Message);
                return "-1";
            }
            return "-1";
        }

        [WebMethod(Description = "Obtener Cliente Informacion por COdigo")]
        public string ObtenerClienteInformacion()
        {
            System.Xml.XmlElement xmlElement;
            try
            {
                String query = @"Select ClientesID,NombreCompleto,RFC,CorreoElectronico,NoTelefono,Codigo
                From Clientes Where EsMayoreo = 1 and esMayoreoAprovado = 1";
                System.Data.DataSet ds = qryToDataSet(query);
                xmlElement = Serialize(ds.Tables[0]);
                return xmlElement.OuterXml.ToString();
            }
            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", ex.Message);
                return "-1";
            }
            return "-1";
        }

        //RAP-----------------------------
        [WebMethod(Description = "Obtener distancia en kilomentro de dos puntos")]
        public string distanciaKM(String sLat1, String sLon1, String sLat2, String sLon2)
        {

            //public const double EarthRadius = 6371;

            double distance = 0;
            double Lat = (double.Parse(sLat2) - double.Parse(sLat1)) * (Math.PI / 180);
            double Lon = (double.Parse(sLon2) - double.Parse(sLon1)) * (Math.PI / 180);
            double a = Math.Sin(Lat / 2) * Math.Sin(Lat / 2) + Math.Cos(double.Parse(sLat1) * (Math.PI / 180)) * Math.Cos(double.Parse(sLat2) * (Math.PI / 180)) * Math.Sin(Lon / 2) * Math.Sin(Lon / 2);
            double c = 2 * Math.Atan2(Math.Sqrt(a), Math.Sqrt(1 - a));
            distance = 6371 * c;
            return (distance * 1.20).ToString();// mandar  kilometros




        }

        [WebMethod(Description = "Obtener Pedido RAP")]
        public string RAP_Pedidos(String sPedido)
        {
            String sQry = @"
                            SELECT 
                            CATSAT_ClaveProductosServicios.ClaveProdServ,
                            CATSAT_ClaveProductosServicios.Descripcion,
                            sum(Pedidos_Articulos.PiezasSurtidas) as Cantidad,
                            Articulos.PermitirDecimales as EsKilogramo,
                            Articulos.CATSAT_ClaveUnidadID as ClaveUnidadSAT,
                            sum (Articulos.PesoPromedio* Pedidos_Articulos.PiezasSurtidas) as PesoTotal,
                            sum( Pedidos_Articulos.ImporteIVACosto) as importeCostoIVA
                            from Pedidos_Articulos,Articulos,Pedidos,CATSAT_ClaveProductosServicios
                            where
                            Pedidos.PedidosID=Pedidos_Articulos.PedidosID
                            and Pedidos_Articulos.ArticulosID=Articulos.ArticulosID
                            and Articulos.CATSAT_ClaveProductosServiciosID=CATSAT_ClaveProductosServicios.CATSAT_ClaveProductosServiciosID
                            and Pedidos_Articulos.PedidosID=" + sPedido + @"
                            GROUP by 
                            CATSAT_ClaveProductosServicios.ClaveProdServ,
                            CATSAT_ClaveProductosServicios.Descripcion,
                            Articulos.PermitirDecimales ";



            System.Data.DataSet ds;
            System.Xml.XmlElement xmlElement;
            try
            {
                ds = qryToDataSet(sQry);
                if (ds.Tables.Count > 0)
                {
                    xmlElement = Serialize(ds.Tables[0]);
                    return xmlElement.OuterXml.ToString();
                }
                return "-1";
            }
            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "Regresa solicitudes de cancelacion:" + ex.Message + ex.StackTrace + "\n" + sQry);
                return "Ocurrio un error inesperado";
            }



        }
        [WebMethod(Description = "Obtener Pedido RAP Por articulo")]
        public string RAP_Pedidos_PorArticulo(String sPedido)
        {
            String sQry = @"
                            SELECT 
                            CATSAT_ClaveProductosServicios.ClaveProdServ,
                            CATSAT_ClaveProductosServicios.Descripcion,
                            Pedidos_Articulos.PiezasSurtidas as Cantidad,
                            CATSAT_ClaveUnidad.ClaveUnidad as ClaveUnidadSAT,
                            Pedidos_Articulos.ArticulosID as ArticulosID,
                            Articulos.Codigo as Codigo,
                            Articulos.Nombre as descripcion_Articulo,
                            Articulos.PermitirDecimales as EsKilogramo,
                            (Articulos.PesoPromedio* Pedidos_Articulos.PiezasSurtidas) as PesoTotal,
                             Pedidos_Articulos.ImporteIVACosto as importeCostoIVA
                            from Pedidos_Articulos,Articulos,Pedidos,CATSAT_ClaveProductosServicios,CATSAT_ClaveUnidad
                            where
                            Pedidos.PedidosID=Pedidos_Articulos.PedidosID
							and Articulos.CATSAT_ClaveUnidadID=CATSAT_ClaveUnidad.CATSAT_ClaveUnidadID
                            and Pedidos_Articulos.ArticulosID=Articulos.ArticulosID
                            and Articulos.CATSAT_ClaveProductosServiciosID=CATSAT_ClaveProductosServicios.CATSAT_ClaveProductosServiciosID
                            and Pedidos_Articulos.PedidosID=" + sPedido + @"
                            and Pedidos_Articulos.PiezasSurtidas>0
                             ";



            System.Data.DataSet ds;
            System.Xml.XmlElement xmlElement;
            try
            {
                ds = qryToDataSet(sQry);
                if (ds.Tables.Count > 0)
                {
                    xmlElement = Serialize(ds.Tables[0]);
                    return xmlElement.OuterXml.ToString();
                }
                return "-1";
            }
            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "Regresa solicitudes de cancelacion:" + ex.Message + ex.StackTrace + "\n" + sQry);
                return "Ocurrio un error inesperado";
            }



        }
        [WebMethod(Description = "Obtener Transferencias RAP")]
        public string RAP_Transferencias_Salidas(String sTransferencias)
        {
            String sQry = @"
                            SELECT 
                            CATSAT_ClaveProductosServicios.ClaveProdServ,
                            CATSAT_ClaveProductosServicios.Descripcion,
                            sum(SalidasArticulos.Cantidad) as Cantidad,
                            Articulos.PermitirDecimales as EsKilogramo,
                            Articulos.CATSAT_ClaveUnidadID as ClaveUnidadSAT,
                            sum (Articulos.PesoPromedio* SalidasArticulos.Cantidad) as PesoTotal,
                            sum(SalidasArticulos.ImporteIVACosto) as importeCostoIVA
                            from SalidasArticulos,Articulos,salidas,CATSAT_ClaveProductosServicios
                            where
                            salidas.SalidasID=SalidasArticulos.SalidasID
                            and SalidasArticulos.ArticulosID=Articulos.ArticulosID
                            and Articulos.CATSAT_ClaveProductosServiciosID=CATSAT_ClaveProductosServicios.CATSAT_ClaveProductosServiciosID
                            and SalidasArticulos.SalidasID=" + sTransferencias + @"
                            and Salidas.TiposMovimientosID=8001
                            GROUP by 
                            CATSAT_ClaveProductosServicios.ClaveProdServ,
                            CATSAT_ClaveProductosServicios.Descripcion,
                            Articulos.PermitirDecimales  ";



            System.Data.DataSet ds;
            System.Xml.XmlElement xmlElement;
            try
            {
                ds = qryToDataSet(sQry);
                if (ds.Tables.Count > 0)
                {
                    xmlElement = Serialize(ds.Tables[0]);
                    return xmlElement.OuterXml.ToString();
                }
                return "-1";
            }
            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "Regresa solicitudes de cancelacion:" + ex.Message + ex.StackTrace + "\n" + sQry);
                return "Ocurrio un error inesperado";
            }


        }
        [WebMethod(Description = "Obtener Transferencias RAP Por Articulos")]
        public string RAP_Transferencias_Salidas_Por_Articulos(String sTransferencias)
        {
            String sQry = @"
                            SELECT 
                            CATSAT_ClaveProductosServicios.ClaveProdServ,
                            CATSAT_ClaveProductosServicios.Descripcion,
                            SalidasArticulos.ArticulosID as ArticulosID,
                            Articulos.Codigo as Codigo,
                            Articulos.Nombre as descripcion_Articulo,
                            SalidasArticulos.Cantidad as Cantidad,
                            Articulos.PermitirDecimales as EsKilogramo,
                            CATSAT_ClaveUnidad.ClaveUnidad as ClaveUnidadSAT,
                            (Articulos.PesoPromedio* SalidasArticulos.Cantidad) as PesoTotal,
                            SalidasArticulos.ImporteIVACosto as importeCostoIVA
                            from Salidas 
                            inner JOIN SalidasArticulos on Salidas.SalidasID = SalidasArticulos.SalidasID
                            inner join Articulos on SalidasArticulos.ArticulosID = Articulos.ArticulosID
						    inner join CATSAT_ClaveUnidad on Articulos.CATSAT_ClaveUnidadID=CATSAT_ClaveUnidad.CATSAT_ClaveUnidadID
                            left join CATSAT_ClaveProductosServicios on Articulos.CATSAT_ClaveProductosServiciosID = CATSAT_ClaveProductosServicios.CATSAT_ClaveProductosServiciosID
                            where
                            SalidasArticulos.SalidasID=" + sTransferencias + @"
                            and Salidas.TiposMovimientosID=8001  ";



            System.Data.DataSet ds;
            System.Xml.XmlElement xmlElement;
            try
            {
                ds = qryToDataSet(sQry);
                if (ds.Tables.Count > 0)
                {
                    xmlElement = Serialize(ds.Tables[0]);
                    return xmlElement.OuterXml.ToString();
                }
                return "-1";
            }
            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "Regresa solicitudes de cancelacion:" + ex.Message + ex.StackTrace + "\n" + sQry);
                return "Ocurrio un error inesperado";
            }


        }



        [WebMethod(Description = "Obtener Encabezados Pedidos RAP")]
        public string RAP_Pedido_Encabezados(String sPedido)
        {
            String sQry = @"
                            SELECT 
                            Sucursales.Nombre as NombreSucural,
                            Pedidos.NoPedido as Folio,
                            Estatus_Pedidos.Nombre as EstatusDescripcion,
                            Pedidos.Fecha as FechaGeneracion
                            from
                            Sucursales, Pedidos, Estatus_Pedidos
                            WHERE
                            Pedidos.PedidosID=" + sPedido + @"
                            and 
                            Pedidos.SucursalesID=Sucursales.SucursalesID
                            and
                            Pedidos.Estatus_PedidosID=Estatus_Pedidos.Estatus_PedidosID  ";



            System.Data.DataSet ds;
            System.Xml.XmlElement xmlElement;
            try
            {
                ds = qryToDataSet(sQry);
                if (ds.Tables.Count > 0)
                {
                    xmlElement = Serialize(ds.Tables[0]);
                    return xmlElement.OuterXml.ToString();
                }
                return "-1";
            }
            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "Regresa solicitudes de cancelacion:" + ex.Message + ex.StackTrace + "\n" + sQry);
                return "Ocurrio un error inesperado";
            }


        }



        [WebMethod(Description = "Obtener Encabezados Transferencia RAP")]
        public string RAP_Transferencia_Encabezados(String sTransferencias)
        {
            String sQry = @"
                            select 
                            Destinos.Nombre as NombreSucural,
                            Salidas.FolioMovimiento as Folio,
                            Salidas.FechaSalida as FechaGeneracion
                            from
                            salidas,Destinos
                            where
                            Salidas.SalidasID=" + sTransferencias + @"
                            and
                            Salidas.DestinosID=Destinos.DestinosID  ";



            System.Data.DataSet ds;
            System.Xml.XmlElement xmlElement;
            try
            {
                ds = qryToDataSet(sQry);
                if (ds.Tables.Count > 0)
                {
                    xmlElement = Serialize(ds.Tables[0]);
                    return xmlElement.OuterXml.ToString();
                }
                return "-1";
            }
            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "Regresa envabezadis transferencuas:" + ex.Message + ex.StackTrace + "\n" + sQry);
                return "Ocurrio un error inesperado";
            }


        }


        //-----------------------[Rap Orden de Compra]------------------
        [WebMethod(Description = "Obtener Orden de Compra por articulo RAP")]
        public string RAP_Orden_Compra_Por_Articulo(String sOrdenCompra)
        {
            String sQry = @"
                                select 
                            CATSAT_ClaveProductosServicios.ClaveProdServ,
                            CATSAT_ClaveProductosServicios.Descripcion,
                            (PedidosComprasArticulos.CantidadPedida + PedidosComprasArticulos.CantidadBonificacion)  as Cantidad,
                            PedidosComprasArticulos.ArticulosID as ArticulosID,
                            Articulos.Codigo as Codigo,
                            Articulos.Nombre as descripcion_Articulo,
                            Articulos.PermitirDecimales as EsKilogramo,
                            CATSAT_ClaveUnidad.ClaveUnidad as ClaveUnidadSAT,
                            (Articulos.PesoPromedio* PedidosComprasArticulos.CantidadPedida + PedidosComprasArticulos.CantidadBonificacion) as PesoTotal,
                            PedidosComprasArticulos.Importe as importeCostoIVA
                            from
                            CATSAT_ClaveProductosServicios,PedidosComprasArticulos,Articulos,PedidosCompras,CATSAT_ClaveUnidad
                            where
                            PedidosCompras.PedidosComprasID = PedidosComprasArticulos.PedidosComprasID
                            and
							CATSAT_ClaveUnidad.CATSAT_ClaveUnidadID=Articulos.CATSAT_ClaveUnidadID
						    and
                            PedidosComprasArticulos.ArticulosID = Articulos.ArticulosID
                            and
                            Articulos.CATSAT_ClaveProductosServiciosID=CATSAT_ClaveProductosServicios.CATSAT_ClaveProductosServiciosID
                            and
                            PedidosCompras.PedidosComprasID=" + sOrdenCompra + @"
                            and
                            (PedidosComprasArticulos.CantidadPedida + PedidosComprasArticulos.CantidadBonificacion)>0
                            ";



            System.Data.DataSet ds;
            System.Xml.XmlElement xmlElement;
            try
            {
                ds = qryToDataSet(sQry);
                if (ds.Tables.Count > 0)
                {
                    xmlElement = Serialize(ds.Tables[0]);
                    return xmlElement.OuterXml.ToString();
                }
                return "-1";
            }
            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "Regresa ordenes de compras articulos:" + ex.Message + ex.StackTrace + "\n" + sQry);
                return "Ocurrio un error inesperado";
            }


        }



        [WebMethod(Description = "Obtener Encabezados Orden Compra RAP")]
        public string RAP_Orden_Compra_Encabezado(String sOrdenCompra)
        {
            String sQry = @"
                                    select 
                                    Sucursales.Nombre as NombreSucural,
                                    PedidosCompras.FolioMovimiento as Folio,
                                    PedidosCompras.FechaHoraCreacion as FechaGeneracion,
                                    Proveedores.Nombre as Nombre
                                    from
                                    PedidosCompras,Sucursales,Proveedores
                                    where
                                    PedidosCompras.SucursalesID = Sucursales.SucursalesID
                                    and
                                    PedidosCompras.PedidosComprasID=" + sOrdenCompra + @"
                                    and
                                    PedidosCompras.ProveedoresID=Proveedores.ProveedoresID
                                    ";

            System.Data.DataSet ds;
            System.Xml.XmlElement xmlElement;
            try
            {
                ds = qryToDataSet(sQry);
                if (ds.Tables.Count > 0)
                {
                    xmlElement = Serialize(ds.Tables[0]);
                    return xmlElement.OuterXml.ToString();
                }
                return "-1";
            }
            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "Regresa ordenes de compras encabezado:" + ex.Message + ex.StackTrace + "\n" + sQry);
                return "Ocurrio un error inesperado";
            }


        }

        [WebMethod(Description = "Obtener articulos por codigo")]
        public string ConsultarArticulo(string CodigoArticulo, string SucursalID)
        {
            string Query;
            System.Data.DataSet ds;
            System.Xml.XmlElement xmlElement;
            Query = @"select 
                        Articulos.ArticulosID,
                        Articulos.Nombre as Descripcion,
                        Articulos.codigo as Codigo,
                        Lineas.NombreLinea,
                        CATSAT_ClaveUnidad.ClaveUnidad,
                        Articulosprecios.PrecioCIVA,
                        CATSAT_Impuestos.ClaveImpuesto as ClaveImpuesto,
                        CATSAT_TiposFactores.TipoFactor,
                        CATSAT_TasasCuotasImpuestos.ValorMaximo as PorcentajeImpuesto,
                        CATSAT_ClaveProductosServicios.ClaveProdServ,
                        Articulos.TiposIEPSID,
                        Articulos.ValorIEPS as valorIEPS,
                        Articulos.CantidadIEPS,
                        Articulos.PesoPromedio,
                        Articulos.activo as Activo
                    from 
                        Articulos,
                        Articulosprecios,
                        Lineas,
                        CATSAT_ClaveUnidad,
                        CATSAT_ClaveProductosServicios,
                        CATSAT_Impuestos,
                        CATSAT_TasasCuotasImpuestos,
                        CATSAT_TiposFactores
                    where
                        Articulos.articulosID = ArticulosPrecios.ArticulosID and
                        Articulos.LineaID = Lineas.LineasID and
                        Articulos.CATSAT_TasasCuotasImpuestosID=CATSAT_TasasCuotasImpuestos.CATSAT_TasasCuotasImpuestosID and
                        CATSAT_Impuestos.Descripcion=CATSAT_TasasCuotasImpuestos.Impuesto and
                        Articulos.CATSAT_ClaveUnidadID = CATSAT_ClaveUnidad.CATSAT_ClaveUnidadID and
                        Articulos.CATSAT_ClaveProductosServiciosID = CATSAT_ClaveProductosServicios.CATSAT_ClaveProductosServiciosID and
                        Articulos.CATSAT_TiposFactoresID=CATSAT_TiposFactores.CATSAT_TiposFactoresID and
                        Articulosprecios.Nivel = 'NV1' and
                        Articulosprecios.SucursalesID = " + SucursalID + @" and
                        Articulos.codigo in (" + CodigoArticulo + ")";
            try
            {
                ds = qryToDataSet(Query);
                if (ds.Tables.Count > 0)
                {
                    xmlElement = Serialize(ds.Tables[0]);
                    return xmlElement.OuterXml.ToString();
                }
                return "";
            }
            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "ConsultarArticulo:" + ex.Message + ex.StackTrace + "\n" + Query);
                return "Ocurrio un error inesperado";
            }
        }
        [WebMethod(Description = "Obtener todos los articulos en venta activos")]
        public string ConsultarArticulos(string SucursalID)
        {
            string Query;
            System.Data.DataSet ds;
            System.Xml.XmlElement xmlElement;
            Query = @"select 
                        articulos.codigo,
                        articulos.nombre
                    from 
                        Articulos,
                        Articulosprecios,
                        Lineas,
                        CATSAT_ClaveUnidad,
                        CATSAT_ClaveProductosServicios,
                        CATSAT_Impuestos,
                        CATSAT_TasasCuotasImpuestos,
                        CATSAT_TiposFactores
                    where
                        Articulos.articulosID = ArticulosPrecios.ArticulosID and
                        Articulos.LineaID = Lineas.LineasID and
                        Articulos.CATSAT_TasasCuotasImpuestosID=CATSAT_TasasCuotasImpuestos.CATSAT_TasasCuotasImpuestosID and
                        CATSAT_Impuestos.Descripcion=CATSAT_TasasCuotasImpuestos.Impuesto and
                        Articulos.CATSAT_ClaveUnidadID = CATSAT_ClaveUnidad.CATSAT_ClaveUnidadID and
                        Articulos.CATSAT_ClaveProductosServiciosID = CATSAT_ClaveProductosServicios.CATSAT_ClaveProductosServiciosID and
                        Articulos.CATSAT_TiposFactoresID=CATSAT_TiposFactores.CATSAT_TiposFactoresID and
                        Articulosprecios.Nivel = 'NV1' and
                        Articulosprecios.SucursalesID = " + SucursalID + @" and 
                        ArticulosPrecios.Activo=1 and
                        Articulos.SeVende = 1 and 
                        Articulos.activo = 1
                        order by nombre asc";
            try
            {
                ds = qryToDataSet(Query);
                if (ds.Tables.Count > 0)
                {
                    xmlElement = Serialize(ds.Tables[0]);
                    return xmlElement.OuterXml.ToString();
                }
                return "";
            }
            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "ConsultarArticulo:" + ex.Message + ex.StackTrace + "\n" + Query);
                return "Ocurrio un error inesperado";
            }
        }
        [WebMethod(Description = "Obtener todos codigos de articulos de una venta especifica")]
        public string consultarCodigosArticulos(string VentaID)
        {
            string Query;
            System.Data.DataSet ds;
            System.Xml.XmlElement xmlElement;
            Query = @"select 
	                    Articulos.ArticulosID, 
                        Articulos.codigo,
                        Ventas_Articulos.precio,
                        CATSAT_ClaveUnidad.ClaveUnidad,
                        CATSAT_Impuestos.ClaveImpuesto as ClaveImpuesto,
                        CATSAT_TiposFactores.TipoFactor,
                        CATSAT_TasasCuotasImpuestos.ValorMaximo as PorcentajeImpuesto,
                        Ventas_Articulos.Cantidad,
                        CATSAT_FormasPago.ClaveFormasPago,
                        Ventas.FechaVenta,
                        Ventas_Articulos.Descuento,
                        Articulos.TiposIEPSID,
                        Articulos.ValorIEPS as valorIEPS,
                        Articulos.CantidadIEPS,
                        Ventas_Articulos.ProductoGrabadoSIMP      
                    from
	                    ventas
	                    inner join Ventas_Articulos on ventas.VentasID = Ventas_Articulos.VentasID
	                    inner join VentasTiposPagos on ventas.VentasID = VentasTiposPagos.VentasID
	                    inner join TiposdePagos on VentasTiposPagos.TiposdePagosID = TiposdePagos.TiposdePagosID
	                    inner join Articulos on Ventas_Articulos.ArticulosID = Articulos.ArticulosID
	                    left  join CATSAT_ClaveUnidad on Articulos.CATSAT_ClaveUnidadID = CATSAT_ClaveUnidad.CATSAT_ClaveUnidadID
	                    left join CATSAT_TasasCuotasImpuestos on Articulos.CATSAT_TasasCuotasImpuestosID = CATSAT_TasasCuotasImpuestos.CATSAT_TasasCuotasImpuestosID
	                    left join CATSAT_Impuestos on CATSAT_Impuestos.Descripcion=CATSAT_TasasCuotasImpuestos.Impuesto
	                    left join CATSAT_TiposFactores on Articulos.CATSAT_TiposFactoresID=CATSAT_TiposFactores.CATSAT_TiposFactoresID
	                    left join CATSAT_FormasPago on TiposdePagos.CATSAT_FormaPagoID = CATSAT_FormasPago.CATSAT_FormasPagoID
                    where
	                    VentasTiposPagos.TiposdePagosID = (
                            select
								VentasTiposPagos.TiposdePagosID
							from 
								VentasTiposPagos,
								TiposdePagos
							where 
								VentasTiposPagos.VentasID = " + VentaID + @" 
                                and VentasTiposPagos.TiposdePagosID = TiposdePagos.TiposdePagosID
							order by VentasTiposPagos.ImporteRecibido desc, TiposdePagos.EsTarjeta desc
							limit 1	) and
                            Ventas.Cancelada = 0 and
                            ventas.VentasID = " + VentaID;
            try
            {
                ds = qryToDataSet(Query);
                if (ds.Tables.Count > 0)
                {
                    xmlElement = Serialize(ds.Tables[0]);
                    return xmlElement.OuterXml.ToString();
                }
                return "";
            }
            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "ConsultarArticulo:" + ex.Message + ex.StackTrace + "\n" + Query);
                return "Ocurrio un error inesperado";
            }
        }


        //----------------[Obtener Empleados SION]------------------
        [WebMethod(Description = "Obtener Empleados SION")]
        public string ObtenerEmpleadosSION()
        {
            String sQry = @"select 
	                            Usuarios.Nombre as Usuarios,	
	                            Empleados.EmpleadosID as UsuarioID,
	                            Empleados.NombreCompleto as NombreCompleto,
	                            Puestos.Nombre as Puesto,
	                            Empleados.FechaIngreso as FechaIngreso
                            from Usuarios
	                            inner join Empleados on Usuarios.EmpleadosID = Empleados.EmpleadosID
	                            inner join Puestos on Empleados.PuestosID = Puestos.PuestosID
                            WHERE
	                            Empleados.Activo=1
                            order by Empleados.NombreCompleto asc";
            System.Data.DataSet ds;
            System.Xml.XmlElement xmlElement;
            try
            {
                ds = qryToDataSet(sQry);
                if (ds.Tables.Count > 0)
                {
                    xmlElement = Serialize(ds.Tables[0]);
                    return xmlElement.OuterXml.ToString();
                }
                return "-1";
            }
            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "Obtener Empleados SION:" + ex.Message + ex.StackTrace + "\n" + sQry);
                return "Ocurrio un error inesperado";
            }


        }

        [WebMethod(Description = "Consultar descuento de una venta")]
        public string ConsultarDescuento(string VentaID)
        {
            string Query;
            System.Data.DataSet ds;
            System.Xml.XmlElement xmlElement;
            Query = @"select 
                        sum(importe) as Descuento
                    from 
                        Ventas_Articulos
                    where 
                        Ventas_Articulos.ArticulosID = -1 and 
                        Ventas_Articulos.VentasID = " + VentaID;
            try
            {
                ds = qryToDataSet(Query);
                if (ds.Tables.Count > 0)
                {
                    xmlElement = Serialize(ds.Tables[0]);
                    return xmlElement.OuterXml.ToString();
                }
                return "";
            }
            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "ConsultarArticulo:" + ex.Message + ex.StackTrace + "\n" + Query);
                return "Ocurrio un error inesperado";
            }
        }


        [WebMethod(Description = "Consultar informacion de la venta")]
        public string ConsultarInformacionVenta(string VentaID, string SucursalID)
        {
            string Query;
            System.Data.DataSet ds;
            System.Xml.XmlElement xmlElement;
            Query = @"select 
                        ventas.FechaVenta,
                        Sucursales.Nombre,
                        Ventas.TotalVenta,
                        ventas.Facturada
                    from 
                        Ventas,
                        Sucursales
                    WHERE
                        ventas.SucursalesID = Sucursales.SucursalesID and
                        ventas.VentasID = " + VentaID + @" and 
                        Sucursales.SucursalesID = " + SucursalID;
            try
            {
                ds = qryToDataSet(Query);
                if (ds.Tables.Count > 0)
                {
                    xmlElement = Serialize(ds.Tables[0]);
                    return xmlElement.OuterXml.ToString();
                }
                return "";
            }
            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "ConsultarArticulo:" + ex.Message + ex.StackTrace + "\n" + Query);
                return "Ocurrio un error inesperado";
            }
        }

        //------------------[Factura Global]-----------------------
        [WebMethod(Description = "Obtener ImpuestosAgrupada")]
        public string Factutracion_X_ImpuestosAgrupada(string Fecha)
        {
            string Query;
            System.Data.DataSet ds;
            System.Xml.XmlElement xmlElement;
            Query = @"SELECT 
	                    Impuestos.NombreIVA AS NombreIVA,	
	                    Impuestos.Porcentaje AS Porcentaje,	
	                    Impuestos.ImpuestosID AS ImpuestosID,
	                    Ventas_Articulos.TiposIEPSID AS TiposIEPSID,	
	                    Ventas_Articulos.ValorIEPS AS ValorIEPS,	
                        TiposIEPS.Descripcion as TipoIEPS,
	                    SUM(ROUND( ( ROUND( Ventas_Articulos.Cantidad ,  3) * ROUND( Ventas_Articulos.Precio ,  2) ) ,  2) ) AS Sum_Subtotal,	
	                    SUM(ROUND( Ventas_Articulos.Importe ,  2) ) AS sum_Importe,	
	                    SUM(ROUND( Ventas_Articulos.ImpuestoImporte ,  2) ) AS sum_ImpuestoImporte,	
	                    SUM(ROUND( Ventas_Articulos.ProductoGrabadoIEPS ,  2) ) AS ProductoGrabadoIEPS,	
	                    SUM(ROUND( Ventas_Articulos.ProductoGrabadoImporte ,  2) ) AS ProductoGrabadoImporte,	
	                    SUM(ROUND( Ventas_Articulos.ProductoGrabadoIVA ,  2) ) AS ProductoGrabadoIVA,	
	                    SUM(ROUND( Ventas_Articulos.ProductoGrabadoSIMP ,  2) ) AS ProductoGrabadoSIMP,	
	                    SUM(ROUND( Ventas_Articulos.CantidadIEPS ,  2) ) AS CantidadIEPS,	
	                    SUM(ROUND( ( Ventas_Articulos.ProductoGrabadoIEPS / Ventas_Articulos.ValorIEPS ) ,  3) ) AS IEPSBaseCuota
                    FROM 
	                    CortesZ,	
	                    CortesY,	
	                    Ventas,	
	                    Ventas_Articulos,	
	                    Impuestos,
	                    TiposIEPS
                    WHERE 
	                    Impuestos.ImpuestosID = Ventas_Articulos.ImpuestoID
	                    AND		Ventas.VentasID = Ventas_Articulos.VentasID
	                    AND		CortesY.CortesYID = Ventas.CortesYID
	                    AND		CortesZ.CortesZID = CortesY.CortesZID
	                    AND 	Ventas.Cancelada = 0
	                    AND		Ventas.Facturada = 0
	                    AND		Ventas.VentaPendiente = 0
	                    AND		TiposIEPS.TiposIEPSID = Ventas_Articulos.TiposIEPSID
	                    AND		CortesZ.FechaCorteZ =" + Fecha + @"
                GROUP BY 
	                Impuestos.NombreIVA,	
	                Impuestos.Porcentaje,	
	                Impuestos.ImpuestosID,	
	                Ventas_Articulos.TiposIEPSID,	
	                Ventas_Articulos.ValorIEPS,
                    TiposIEPS.Descripcion
                ORDER BY 
	                ImpuestosID ASC,	
	                TiposIEPSID ASC,	
	                ValorIEPS ASC";
            try
            {
                ds = qryToDataSet(Query);
                if (ds.Tables.Count > 0)
                {
                    xmlElement = Serialize(ds.Tables[0]);
                    return xmlElement.OuterXml.ToString();
                }
                return "";
            }
            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "Factutracion_X_ImpuestosAgrupada:" + ex.Message + ex.StackTrace + "\n" + Query);
                return "Ocurrio un error inesperado";
            }
        }

        [WebMethod(Description = "Obtener Cortes Z Abiertos")]
        public string Facturacion_ObtenerCortesZ_Abiertos(string Fecha)
        {
            string Query;
            System.Data.DataSet ds;
            System.Xml.XmlElement xmlElement;
            Query = @"SELECT 
	                    CortesZ.FechaCorteZ AS FechaCorteZ,	
	                    CortesZ.EsCerrado AS EsCerrado
                    FROM 
	                    CortesZ
                    WHERE 
	                    CortesZ.EsCerrado = 0
	                    AND	CortesZ.FechaCorteZ = " + Fecha;
            try
            {
                ds = qryToDataSet(Query);
                if (ds.Tables.Count > 0)
                {
                    xmlElement = Serialize(ds.Tables[0]);
                    return xmlElement.OuterXml.ToString();
                }
                return "";
            }
            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "ConsultarArticulo:" + ex.Message + ex.StackTrace + "\n" + Query);
                return "Ocurrio un error inesperado";
            }
        }

        [WebMethod(Description = "Cambiar el estatus de una venta a facturado")]
        public string VentaFacturada(string VentaID)
        {
            string Query;
            System.Data.DataSet ds;
            System.Xml.XmlElement xmlElement;
            Query = @"update 
                        ventas
                    set 
                        ventas.Facturada = 1
                    where 
                        Ventas.VentasID in (" + VentaID + ")";
            try
            {
                ds = qryToDataSet(Query);
                if (ds.Tables.Count > 0)
                {
                    xmlElement = Serialize(ds.Tables[0]);
                    return xmlElement.OuterXml.ToString();
                }
                return "";
            }
            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "ConsultarArticulo:" + ex.Message + ex.StackTrace + "\n" + Query);
                return "Ocurrio un error inesperado";
            }
        }


        [WebMethod(Description = "QRY_cortesYDevoluciones_X_Fecha")]
        public string QRY_cortesYDevoluciones_X_Fecha(string ParamFecha, string ParamImpuestosID)
        {
            string Query;
            System.Data.DataSet ds;
            System.Xml.XmlElement xmlElement;
            Query = @"SELECT 
	                    SUM(CortesYDevoluciones.Total) AS sum_Total,	
	                    SUM(CortesYDevoluciones.IVA) AS sum_IVA,	
	                    SUM(CortesYDevoluciones.TotalSinIVA) AS sum_TotalSinIVA,	
	                    CortesYDevoluciones.ImpuestosID AS ImpuestosID
                    FROM 
	                    CortesZ,	
	                    CortesY,	
	                    CortesYDevoluciones
                    WHERE 
	                    CortesY.CortesYID = CortesYDevoluciones.CortesYID
	                    AND		CortesZ.CortesZID = CortesY.CortesZID
	                    AND
	                    (
		                    CortesZ.FechaCorteZ = " + ParamFecha + @"
		                    AND	CortesYDevoluciones.ImpuestosID =" + ParamImpuestosID + @"		                 
	                    )
                    GROUP BY 
	                    CortesYDevoluciones.ImpuestosID
                    ORDER BY 
	                    ImpuestosID ASC";
            try
            {
                ds = qryToDataSet(Query);
                if (ds.Tables.Count > 0)
                {
                    xmlElement = Serialize(ds.Tables[0]);
                    return xmlElement.OuterXml.ToString();
                }
                return "";
            }
            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "QRY_cortesYDevoluciones_X_Fecha:" + ex.Message + ex.StackTrace + "\n" + Query);
                return "Ocurrio un error inesperado";
            }
        }

        [WebMethod(Description = "QRY_Ventas_ImportePaquete_Sum_FechaCortesZ")]
        public string QRY_Ventas_ImportePaquete_Sum_FechaCortesZ(string ParamFecha, string ParamFacturada)
        {
            string Query;
            System.Data.DataSet ds;
            System.Xml.XmlElement xmlElement;
            Query = @"SELECT 
	                    SUM(Ventas.Importepaquete) AS sum_Importepaquete
                    FROM 
	                    CortesZ,	
	                    CortesY,	
	                    Ventas
                    WHERE 
	                    CortesZ.CortesZID = CortesY.CortesZID
	                    AND		CortesY.CortesYID = Ventas.CortesYID
	                    AND
	                    (
		                    Ventas.Cancelada = 0
		                    AND	Ventas.VentaPaquete = 1
		                    AND	CortesZ.FechaCorteZ = " + ParamFecha + @"
		                
		                    AND	Ventas.Facturada = " + ParamFacturada + @"
	                    )";
            try
            {
                ds = qryToDataSet(Query);
                if (ds.Tables.Count > 0)
                {
                    xmlElement = Serialize(ds.Tables[0]);
                    return xmlElement.OuterXml.ToString();
                }
                return "";
            }
            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "QRY_Ventas_ImportePaquete_Sum_FechaCortesZ:" + ex.Message + ex.StackTrace + "\n" + Query);
                return "Ocurrio un error inesperado";
            }
        }


        [WebMethod(Description = "QRY_ImpuestosCortesY_X_FechaCortesZID")]
        public string QRY_ImpuestosCortesY_X_FechaCortesZID(string ParamFecha)
        {
            string Query;
            System.Data.DataSet ds;
            System.Xml.XmlElement xmlElement;
            Query = @"SELECT 
	                    SUM(ImpuestosCortesY.Cantidad) AS sum_Cantidad,	
	                    SUM(ImpuestosCortesY.Importe) AS sum_Importe,	
	                    CortesZ.FechaCorteZ AS FechaCorteZ
                    FROM 
	                    CortesZ,	
	                    CortesY,	
	                    ImpuestosCortesY
                    WHERE 
	                    CortesY.CortesYID = ImpuestosCortesY.CortesYID
	                    AND		CortesZ.CortesZID = CortesY.CortesZID
	                    AND
	                    (
		                    CortesZ.FechaCorteZ = " + ParamFecha + @"
	                    )
                    GROUP BY 
	                    CortesZ.FechaCorteZ";
            try
            {
                ds = qryToDataSet(Query);
                if (ds.Tables.Count > 0)
                {
                    xmlElement = Serialize(ds.Tables[0]);
                    return xmlElement.OuterXml.ToString();
                }
                return "";
            }
            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "QRY_ImpuestosCortesY_X_FechaCortesZID:" + ex.Message + ex.StackTrace + "\n" + Query);
                return "Ocurrio un error inesperado";
            }
        }


        [WebMethod(Description = "QRY_CortesYDevoluciones_ImpuestosID_FechaCortesZID")]
        public string QRY_CortesYDevoluciones_ImpuestosID_FechaCortesZID(string ParamFecha)
        {
            string Query;
            System.Data.DataSet ds;
            System.Xml.XmlElement xmlElement;
            Query = @"SELECT 
	                    SUM(CortesYDevoluciones.Total) AS sum_Total
                    FROM 
	                    CortesZ,	
	                    CortesY,	
	                    CortesYDevoluciones
                    WHERE 
	                    CortesY.CortesYID = CortesYDevoluciones.CortesYID
	                    AND		CortesZ.CortesZID = CortesY.CortesZID
	                    AND
	                    (
		                    CortesZ.FechaCorteZ = " + ParamFecha + ")";
            try
            {
                ds = qryToDataSet(Query);
                if (ds.Tables.Count > 0)
                {
                    xmlElement = Serialize(ds.Tables[0]);
                    return xmlElement.OuterXml.ToString();
                }
                return "";
            }
            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "QRY_CortesYDevoluciones_ImpuestosID_FechaCortesZID:" + ex.Message + ex.StackTrace + "\n" + Query);
                return "Ocurrio un error inesperado";
            }
        }


        [WebMethod(Description = "QRY_FAC_TotalFacturado_x_FechasSucursalesID")]
        public string QRY_FAC_TotalFacturado_x_FechasSucursalesID(string ParamFecha)
        {
            string Query;
            System.Data.DataSet ds;
            System.Xml.XmlElement xmlElement;
            Query = @"select 
	                    sum(ventas.TotalVenta)
                    from
	                    ventas
                    where
	                    Ventas.Cancelada = 0
	                    and Ventas.Facturada = 1 
	                    and Ventas.FechaVenta = " + ParamFecha;

            try
            {
                ds = qryToDataSet(Query);
                if (ds.Tables.Count > 0)
                {
                    xmlElement = Serialize(ds.Tables[0]);
                    return xmlElement.OuterXml.ToString();
                }
                return "";
            }
            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "QRY_FAC_TotalFacturado_x_FechasSucursalesID:" + ex.Message + ex.StackTrace + "\n" + Query);
                return "Ocurrio un error inesperado";
            }
        }



        [WebMethod(Description = "QRY_Consulta_VentasServicio_X_CorteIDZ")]
        public string QRY_Consulta_VentasServicio_X_CorteIDZ(string ParamFecha)
        {
            string Query;
            System.Data.DataSet ds;
            System.Xml.XmlElement xmlElement;
            Query = @"SELECT DISTINCT 
	                    Ventas.VentasID AS VentasID,	
	                    Ventas.TotalVenta AS TotalVenta
                    FROM 
	                    Ventas,	
	                    CortesY,	
	                    Ventas_Articulos,	
	                    CortesZ
                    WHERE 
	                    Ventas.VentasID = Ventas_Articulos.VentasID
	                    AND Ventas.CortesYID = CortesY.CortesYID
	                    AND	CortesY.CortesZID = CortesZ.CortesZID
		                AND	Ventas_Articulos.esServicio = 1
		                AND	Ventas.Facturada = 1
		                AND	CortesZ.FechaCorteZ = " + ParamFecha;
            try
            {
                ds = qryToDataSet(Query);
                if (ds.Tables.Count > 0)
                {
                    xmlElement = Serialize(ds.Tables[0]);
                    return xmlElement.OuterXml.ToString();
                }
                return "";
            }
            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "QRY_Consulta_VentasServicio_X_CorteIDZ:" + ex.Message + ex.StackTrace + "\n" + Query);
                return "Ocurrio un error inesperado";
            }
        }




        [WebMethod(Description = "QRY_FAC_Factutracion_X_Impuestos_VentasID")]
        public string QRY_FAC_Factutracion_X_Impuestos_VentasID(string ParamFecha)
        {
            string Query;
            System.Data.DataSet ds;
            System.Xml.XmlElement xmlElement;
            Query = @"SELECT 
	                    Impuestos.NombreIVA AS NombreIVA,	
	                    Impuestos.Porcentaje AS Porcentaje,	
	                    Impuestos.ImpuestosID AS ImpuestosID,	
	                    Ventas.VentasID AS VentasID,
                        CortesZ.CortesZID
                    FROM 
	                    CortesZ,	
	                    CortesY,	
	                    Ventas,	
	                    ImpuestosCortesY,	
	                    Impuestos
                    WHERE 
	                    Impuestos.ImpuestosID = ImpuestosCortesY.ImpuestosID
	                    AND		CortesY.CortesYID = ImpuestosCortesY.CortesYID
	                    AND		CortesY.CortesYID = Ventas.CortesYID
	                    AND		CortesZ.CortesZID = CortesY.CortesZID
	                    AND
	                    (
		                    Ventas.Cancelada = 0
		                    AND	Ventas.Facturada = 0
		                    AND	Ventas.VentaPendiente = 0
		                    AND	CortesZ.FechaCorteZ = " + ParamFecha + ")";
            try
            {
                ds = qryToDataSet(Query);
                if (ds.Tables.Count > 0)
                {
                    xmlElement = Serialize(ds.Tables[0]);
                    return xmlElement.OuterXml.ToString();
                }
                return "";
            }
            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "QRY_FAC_Factutracion_X_Impuestos_VentasID:" + ex.Message + ex.StackTrace + "\n" + Query);
                return "Ocurrio un error inesperado";
            }
        }



        [WebMethod(Description = "QRY_FAC_FormasPago_X_VentasTiposPagos")]
        public string QRY_FAC_FormasPago_X_VentasTiposPagos(string ParamVentasIDs)
        {
            string Query;
            System.Data.DataSet ds;
            System.Xml.XmlElement xmlElement;
            Query = @"SELECT 
	                    CATSAT_FormasPago.CATSAT_FormasPagoID AS CATSAT_FormasPagoID,	
	                    CATSAT_FormasPago.ClaveFormasPago AS ClaveFormasPago,	
	                    CATSAT_FormasPago.Descripcion AS Descripcion,	
	                    SUM(VentasTiposPagos.ImporteRecibido) AS sum_ImporteRecibido
                    FROM 
	                    CATSAT_FormasPago,	
	                    Ventas,	
	                    TiposdePagos,	
	                    VentasTiposPagos
                    WHERE 
	                    Ventas.VentasID = VentasTiposPagos.VentasID
	                    AND		VentasTiposPagos.TiposdePagosID = TiposdePagos.TiposdePagosID
	                    AND		TiposdePagos.CATSAT_FormaPagoID = CATSAT_FormasPago.CATSAT_FormasPagoID
	                    AND
	                    (
		                    Ventas.VentasID IN (" + ParamVentasIDs + @") 
	                    )
                    GROUP BY 
	                    CATSAT_FormasPago.CATSAT_FormasPagoID,	
	                    CATSAT_FormasPago.ClaveFormasPago,	
	                    CATSAT_FormasPago.Descripcion
                    ORDER BY 
	                    sum_ImporteRecibido DESC";
            try
            {
                ds = qryToDataSet(Query);
                if (ds.Tables.Count > 0)
                {
                    xmlElement = Serialize(ds.Tables[0]);
                    return xmlElement.OuterXml.ToString();
                }
                return "";
            }
            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "QRY_FAC_FormasPago_X_VentasTiposPagos:" + ex.Message + ex.StackTrace + "\n" + Query);
                return "Ocurrio un error inesperado";
            }
        }


        [WebMethod(Description = "QRY_FAC_CortesYTiposPagos_FechaSucursal")]
        public string QRY_FAC_CortesYTiposPagos_FechaSucursal(string ParamCortesZID)
        {
            string Query;
            System.Data.DataSet ds;
            System.Xml.XmlElement xmlElement;
            Query = @"SELECT 
	                    CortesYTiposPagos.TipoCambio AS TipoCambio,	
	                    TiposdePagos.Descripcion AS Descripcion,	
	                    CortesY.CortesZID AS CortesZID,	
	                    CortesZ.FechaCorteZ AS FechaCorteZ,	
	                    CortesZ.SucursalesID AS SucursalesID,	
	                    TiposPagoFacturas.Codigo AS Codigo,	
	                    SUM(CortesYTiposPagos.ImporteRecibido) AS sum_ImporteRecibido
                    FROM 
	                    CortesZ,	
	                    CortesY,	
	                    CortesYTiposPagos,	
	                    TiposdePagos,	
	                    TiposPagoFacturas
                    WHERE 
	                    CortesZ.CortesZID = CortesY.CortesZID
	                    AND		CortesY.CortesYID = CortesYTiposPagos.CortesYID
	                    AND		TiposdePagos.TiposdePagosID = CortesYTiposPagos.TiposdePagosID
	                    AND		TiposPagoFacturas.TiposPagoFacturasID = TiposdePagos.TiposPagoFacturasID
	                    AND
	                    (
		                    CortesY.CortesZID = " + ParamCortesZID + @"
	                    )
                    GROUP BY 
	                    CortesYTiposPagos.TipoCambio,	
	                    TiposdePagos.Descripcion,	
	                    CortesY.CortesZID,	
	                    CortesZ.FechaCorteZ,	
	                    CortesZ.SucursalesID,	
	                    TiposPagoFacturas.Codigo
                    ORDER BY 
	                    sum_ImporteRecibido ASC";
            try
            {
                ds = qryToDataSet(Query);
                if (ds.Tables.Count > 0)
                {
                    xmlElement = Serialize(ds.Tables[0]);
                    return xmlElement.OuterXml.ToString();
                }
                return "";
            }
            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "QRY_FAC_CortesYTiposPagos_FechaSucursal:" + ex.Message + ex.StackTrace + "\n" + Query);
                return "Ocurrio un error inesperado";
            }
        }


        [WebMethod(Description = "QRY_ConfiguracionInpuestos")]
        public string QRY_ConfiguracionInpuestos(string ParamFecha, string ParamFacturacionGlobal)
        {
            string Query;
            System.Data.DataSet ds;
            System.Xml.XmlElement xmlElement;
            Query = @"SELECT 
	                    ImpuestosCortesZ.ImpuestosCortesZID AS ImpuestosCortesZID,	
	                    ImpuestosCortesZ.Fecha AS Fecha,	
	                    ImpuestosCortesZ.Base AS Base,	
	                    ImpuestosCortesZ.BaseIVA AS BaseIVA,	
	                    ImpuestosCortesZ.BaseIEPS AS BaseIEPS,	
	                    ImpuestosCortesZ.IEPSIVA AS IEPSIVA,	
	                    ImpuestosCortesZ.IVA AS IVA,	
	                    ImpuestosCortesZ.FacturaGlobal AS FacturaGlobal,	
	                    ImpuestosCortesZ.EmpleadosID AS EmpleadosID,	
	                    ImpuestosCortesZ.FechaRegistro AS FechaRegistro,	
	                    Empleados.NombreCompleto AS NombreCompleto
                    FROM 
	                    Empleados,	
	                    ImpuestosCortesZ
                    WHERE 
	                    Empleados.EmpleadosID = ImpuestosCortesZ.EmpleadosID
	                    AND ImpuestosCortesZ.Fecha =" + ParamFecha + @"
		                AND	ImpuestosCortesZ.FacturaGlobal = " + ParamFacturacionGlobal;
            try
            {
                ds = qryToDataSet(Query);
                if (ds.Tables.Count > 0)
                {
                    xmlElement = Serialize(ds.Tables[0]);
                    return xmlElement.OuterXml.ToString();
                }
                return "";
            }
            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "QRY_ConfiguracionInpuestos:" + ex.Message + ex.StackTrace + "\n" + Query);
                return "Ocurrio un error inesperado";
            }
        }


        [WebMethod(Description = "AutofacturacionDatosTicket")]
        public string AutofacturacionDatosTicket(string VentasID)
        {
            string Query;
            System.Data.DataSet ds;
            System.Xml.XmlElement xmlElement;
            Query = @"select
	                    CATSAT_FormasPago.Descripcion as FormaPago,
	                    CATSAT_FormasPago.ClaveFormasPago as ClaveFormaPago,
	                    ventas.Subtotal as Subtotal,
	                    (ventas.TotalVenta - ventas.Subtotal) as Impuesto,
	                    ventas.TotalVenta as TotalVenta,
	                    ventas.Facturada as Facturado,
	                    ventas.Cancelada as Cancelado,
	                    ventas.FechaHora as FechaHoraGeneracion,
	                    ventas.SucursalesID as SucursalID,
	                    VentasTiposPagos.ImporteRecibido as ImporteRecibido
                    from 
	                    VentasTiposPagos,
	                    TiposdePagos, 
	                    CATSAT_FormasPago,
	                    ventas
                    where 
	                    ventas.VentasID=" + VentasID + @"
		            and
	                    VentasTiposPagos.TiposdePagosID = TiposdePagos.TiposdePagosID and
	                    TiposdePagos.CATSAT_FormaPagoID = CATSAT_FormasPago.CATSAT_FormasPagoID and
	                    VentasTiposPagos.VentasID = ventas.VentasID 
                    order by ImporteRecibido desc
                    limit 1";
            try
            {
                ds = qryToDataSet(Query);
                if (ds.Tables.Count > 0)
                {
                    xmlElement = Serialize(ds.Tables[0]);
                    return xmlElement.OuterXml.ToString();
                }
                return "";
            }
            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "QRY_ConfiguracionInpuestos:" + ex.Message + ex.StackTrace + "\n" + Query);
                return "Ocurrio un error inesperado";
            }
        }

        //--------------------------[Rastreo de articulos]---------------------------

        [WebMethod(Description = "Rastreo de Articulos conteos")]
        public string RastreoArticulos_Conteos(string ArticuloID, string FechaInicial, string FechaFinal)
        {
            string Query;
            System.Data.DataSet ds;
            System.Xml.XmlElement xmlElement;
            Query = @"select DISTINCT
	                    sum(ConteosArticulos.Diferencia) as ResultadoInventario,
	                    avg(ConteosArticulos.CostoConIva) as CostoConIVA,
	                    sum(ConteosArticulos.Diferencia*ConteosArticulos.CostoConIva) as ImporteAfectacion
                    from
	                    conteos,
	                    ConteosArticulos
                    where   
                        conteos.ConteosID = ConteosArticulos.ConteosID         	                   
                        and ConteosArticulos.ArticulosID =" + ArticuloID + @"
                        and Conteos.Fecha BETWEEN '" + FechaInicial + "' and '" + FechaFinal + @"'
                        and Conteos.EsBorrador = 0";
            try
            {
                ds = qryToDataSet(Query);
                if (ds.Tables.Count > 0)
                {
                    xmlElement = Serialize(ds.Tables[0]);
                    return xmlElement.OuterXml.ToString();
                }
                return "";
            }
            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "RastreoArticulos_Conteos:" + ex.Message + ex.StackTrace + "\n" + Query);
                return "Ocurrio un error inesperado";
            }
        }


        [WebMethod(Description = "Rastreo de Articulos Concentrador")]
        public string RastreoArticulos_Concentrador(string ArticuloID, string FechaInicial, string FechaFinal)
        {
            string Query;
            System.Data.DataSet ds;
            System.Xml.XmlElement xmlElement;
            Query = @"SELECT
	                    sum (ConcentradosArticulosDia.ImporteCostoCIVA) as Concentrador,
                        sum(ConcentradosArticulosDia.Cantidad) as Cantidad
                    from
	                    ConcentradosArticulosDia	
                    where
	                    ConcentradosArticulosDia.ArticulosID = " + ArticuloID + @"
                        and ConcentradosArticulosDia.Dia BETWEEN '" + FechaInicial + "' and '" + FechaFinal + @"'
                    ";
            try
            {
                ds = qryToDataSet(Query);
                if (ds.Tables.Count > 0)
                {
                    xmlElement = Serialize(ds.Tables[0]);
                    return xmlElement.OuterXml.ToString();
                }
                return "";
            }
            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "RastreoArticulos_Concentrador:" + ex.Message + ex.StackTrace + "\n" + Query);
                return "Ocurrio un error inesperado";
            }
        }


        [WebMethod(Description = "Rastreo de Articulos devoluciones")]
        public string RastreoArticulos_Devoluciones(string ArticuloID, string FechaInicial, string FechaFinal)
        {
            string Query;
            System.Data.DataSet ds;
            System.Xml.XmlElement xmlElement;
            Query = @"SELECT 
                        SUM (CortesYDevoluciones.Cantidad*CortesYDevoluciones.CostoCIVA) AS ImporteDevolucion
                    FROM 
	                    CortesY,	
	                    CortesYDevoluciones
                    WHERE 
	                    CortesY.CortesYID = CortesYDevoluciones.CortesYID
	                    AND CortesYDevoluciones.ArticulosID = " + ArticuloID + @"
                        and CortesY.Fecha BETWEEN '" + FechaInicial + "' and '" + FechaFinal + @"'
                    group by ArticulosID";
            try
            {
                ds = qryToDataSet(Query);
                if (ds.Tables.Count > 0)
                {
                    xmlElement = Serialize(ds.Tables[0]);
                    return xmlElement.OuterXml.ToString();
                }
                return "";
            }
            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "RastreoArticulos_Devoluciones:" + ex.Message + ex.StackTrace + "\n" + Query);
                return "Ocurrio un error inesperado";
            }
        }

        [WebMethod(Description = "Rastreo de Articulos UltimoConteo Info")]
        public string RastreoArticulos_UltimoConteo(string ArticuloID)
        {
            string Query;
            System.Data.DataSet ds;
            System.Xml.XmlElement xmlElement;
            Query = @"SELECT DISTINCT 
	                    Empleados.NombreCompleto,	
	                    MAX(Conteos.Fecha) AS UltimaFecha
                    FROM
	                    Conteos, 
	                    ConteosArticulos,
	                    Empleados
                    WHERE
	                    Conteos.ConteosID = ConteosArticulos.ConteosID
                        AND Conteos.EsBorrador = 0
                        AND ConteosArticulos.ArticulosID = " + ArticuloID + @"
                       and Empleados.EmpleadosID = Conteos.UsuarioConto
                    GROUP BY
	                    ConteosArticulos.ArticulosID,
	                    Empleados.NombreCompleto
                    order by ultimaFecha desc
                    limit 1";
            try
            {
                ds = qryToDataSet(Query);
                if (ds.Tables.Count > 0)
                {
                    xmlElement = Serialize(ds.Tables[0]);
                    return xmlElement.OuterXml.ToString();
                }
                return "";
            }
            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "RastreoArticulos_UltimoConteo:" + ex.Message + ex.StackTrace + "\n" + Query);
                return "Ocurrio un error inesperado";
            }
        }


        [WebMethod(Description = "Cambiar el estatus de una venta a NO facturado")]
        public string VentaNoFacturada(string VentaID)
        {
            string Query;
            System.Data.DataSet ds;
            System.Xml.XmlElement xmlElement;
            Query = @"update 
                        ventas
                    set 
                        ventas.Facturada = 0
                    where 
                        Ventas.VentasID in (" + VentaID + ")";
            try
            {
                ds = qryToDataSet(Query);
                if (ds.Tables.Count > 0)
                {
                    xmlElement = Serialize(ds.Tables[0]);
                    return xmlElement.OuterXml.ToString();
                }
                return "";
            }
            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "VentaNoFacturada:" + ex.Message + ex.StackTrace + "\n" + Query);
                return "Ocurrio un error inesperado";
            }
        }


        [WebMethod(Description = "Rastreo de Articulos ultima Existencia")]
        public string RastreoArticulos_Existencia(string ArticuloID, string SucursalID)
        {
            string Query;
            System.Data.DataSet ds;
            System.Xml.XmlElement xmlElement;
            Query = @"select 
                        ArticulosExistencias.ArticulosID,
                        ArticulosExistencias.Existencia 
						from  ArticulosExistencias
						where 
						ArticulosExistencias.ArticulosID IN (" + ArticuloID + ")" + @" 
                       and ArticulosExistencias.SucursalesID =" + SucursalID;
            try
            {
                ds = qryToDataSet(Query);
                if (ds.Tables.Count > 0)
                {
                    xmlElement = Serialize(ds.Tables[0]);
                    return xmlElement.OuterXml.ToString();
                }
                return "";
            }
            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "RastreoArticulos_Existencia:" + ex.Message + ex.StackTrace + "\n" + Query);
                return "Ocurrio un error inesperado";
            }
        }

        //------------------[Rastreo Articulos Linea]-----------------------
        [WebMethod(Description = "Obtener Articulos Por Linea")]
        public string ObtenerArticulos_X_Linea(string LineaID, string Descatalogado, string Activo)
        {
            string Query;
            System.Data.DataSet ds;
            System.Xml.XmlElement xmlElement;
            Query = @"select
	                    Articulos.ArticulosID as ArticuloID,
	                    Articulos.Codigo as Codigo,
	                    Articulos.Nombre as Descripcion
                    from
	                    Articulos
                    where
	                    Articulos.Activo = " + Activo;
            if (LineaID != "-1")
            {
                Query += " and Articulos.LineaID = " + LineaID;
            }
            if (Descatalogado != "2")
            {
                Query += " and Articulos.Descatalogado=" + Descatalogado;
            }
            Query += "order by Descripcion asc";

            try
            {
                ds = qryToDataSet(Query);
                if (ds.Tables.Count > 0)
                {
                    xmlElement = Serialize(ds.Tables[0]);
                    return xmlElement.OuterXml.ToString();
                }
                return "";
            }
            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "ObtenerArticulos_X_Linea:" + ex.Message + ex.StackTrace + "\n" + Query);
                return "Ocurrio un error inesperado";
            }
        }


        [WebMethod(Description = "Obtener lo vendido y sus cantidades")]
        public string ConsultarConcentrador_X_ArticulosID(string SucursalID, string ArticulosID, string FechaInicio, string FechaFin)
        {
            string Query;
            System.Data.DataSet ds;
            System.Xml.XmlElement xmlElement;
            Query = @"SELECT 
	                    SUM(ConcentradosArticulosDia.ImporteCostoCIVA) AS sum_ImporteCostoCIVA,	
	                    SUM(ConcentradosArticulosDia.ImporteCostoSIVA) AS sum_ImporteCostoSIVA,	
	                    SUM(ConcentradosArticulosDia.ImportePrecioCIVA) AS sum_ImportePrecioCIVA,	
	                    SUM(ConcentradosArticulosDia.ImportePrecioSIVA) AS sum_ImportePrecioSIVA,	
	                    SUM(ConcentradosArticulosDia.Cantidad) AS sum_Cantidad,	
	                    ConcentradosArticulosDia.ArticulosID AS ArticulosID
                    FROM 
	                    ConcentradosArticulosDia
                    WHERE 
	                    ConcentradosArticulosDia.SucursalesID = " + SucursalID + @"
	                    AND	ConcentradosArticulosDia.ArticulosID IN (" + ArticulosID + ")" + @" 
	                    AND	ConcentradosArticulosDia.Dia BETWEEN '" + FechaInicio + "' AND '" + FechaFin + @"'
                    GROUP BY 
	                    ConcentradosArticulosDia.ArticulosID";
            try
            {
                ds = qryToDataSet(Query);
                if (ds.Tables.Count > 0)
                {
                    xmlElement = Serialize(ds.Tables[0]);
                    return xmlElement.OuterXml.ToString();
                }
                return "";
            }
            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "ConsultarConcentrador_X_ArticulosID:" + ex.Message + ex.StackTrace + "\n" + Query);
                return "Ocurrio un error inesperado";
            }
        }


        [WebMethod(Description = "QRY donde se obtiene los datos de lo que se ha comprado al proveedor")]
        public string ConsultarPedidos_X_Fecha_Articulo(string SucursalID, string ArticulosID, string FechaInicio, string FechaFin, string PedidosID)
        {
            string Query;
            System.Data.DataSet ds;
            System.Xml.XmlElement xmlElement;
            Query = @"SELECT 
	                    Pedidos_Articulos.ArticulosID AS ArticulosID,	
	                    SUM(Pedidos_Articulos.PiezasSurtidas) AS ArticulosSurtidos,	
	                    SUM(Pedidos_Articulos.ImporteCosto) AS ImporteCosto,	
	                    SUM(Pedidos_Articulos.ImporteIVACosto) AS ImporteIVACosto
                    FROM 
	                    Pedidos,	
	                    Pedidos_Articulos
                    WHERE 
	                    Pedidos.PedidosID = Pedidos_Articulos.PedidosID
	                    AND Pedidos.SucursalesID = " + SucursalID + @"
	                    AND	Pedidos.FechaRecibido BETWEEN '" + FechaInicio + "' AND '" + FechaFin + @"'
	                    AND	Pedidos.Estatus_PedidosID IN (" + PedidosID + @")
	                    AND	Pedidos_Articulos.ArticulosID IN (" + ArticulosID + @") 
	                    AND	Pedidos_Articulos.PiezasSurtidas > 0	
                    GROUP BY 
	                    Pedidos_Articulos.ArticulosID";
            try
            {
                ds = qryToDataSet(Query);
                if (ds.Tables.Count > 0)
                {
                    xmlElement = Serialize(ds.Tables[0]);
                    return xmlElement.OuterXml.ToString();
                }
                return "";
            }
            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "ConsultarPedidos_X_Fecha_Articulo:" + ex.Message + ex.StackTrace + "\n" + Query);
                return "Ocurrio un error inesperado";
            }
        }




        [WebMethod(Description = "Obtener todos codigos de articulos de una Preventa especifica")]
        public string consultarCodigosArticulosPreventa(string PreventaID)
        {
            string Query;
            System.Data.DataSet ds;
            System.Xml.XmlElement xmlElement;
            Query = @"SELECT
                        Articulos.ArticulosID,
                        Articulos.codigo,
                        PreVenta_Mayoreo_Detalle.PrecioUSIVA_Cliente,
                        Preventa_Mayoreo_detalle.Importe,
                        PreVenta_Mayoreo_Detalle.Cantidad,
                        Articulos.TiposIEPSID,
                        Articulos.ValorIEPS as valorIEPS,
                        Articulos.CantidadIEPS,  
                        CATSAT_ClaveUnidad.ClaveUnidad,
                        CATSAT_Impuestos.ClaveImpuesto as ClaveImpuesto,
                        CATSAT_TiposFactores.TipoFactor,
                        CATSAT_TasasCuotasImpuestos.ValorMaximo as PorcentajeImpuesto,
                        PreVenta_Mayoreo.FechaHora
                    FROM
                        PreVenta_Mayoreo,
                        PreVenta_Mayoreo_Detalle,
                        articulos,
                        CATSAT_ClaveUnidad,
                        CATSAT_Impuestos,
                        CATSAT_TasasCuotasImpuestos,
                        CATSAT_TiposFactores
                    WHERE 
                        PreVenta_Mayoreo.PreVenta_MayoreoID = PreVenta_Mayoreo_Detalle.PreVenta_MayoreoID and
                        PreVenta_Mayoreo_Detalle.ArticulosID = Articulos.ArticulosID and
                        Articulos.CATSAT_ClaveUnidadID = CATSAT_ClaveUnidad.CATSAT_ClaveUnidadID and
                        CATSAT_Impuestos.Descripcion=CATSAT_TasasCuotasImpuestos.Impuesto and
                        Articulos.CATSAT_TasasCuotasImpuestosID = CATSAT_TasasCuotasImpuestos.CATSAT_TasasCuotasImpuestosID and
                        Articulos.CATSAT_TiposFactoresID=CATSAT_TiposFactores.CATSAT_TiposFactoresID and
                        PreVenta_Mayoreo.PreVenta_MayoreoID = " + PreventaID;
            try
            {
                ds = qryToDataSet(Query);
                if (ds.Tables.Count > 0)
                {
                    xmlElement = Serialize(ds.Tables[0]);
                    return xmlElement.OuterXml.ToString();
                }
                return "";
            }
            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "ConsultarArticulo:" + ex.Message + ex.StackTrace + "\n" + Query);
                return "Ocurrio un error inesperado";
            }
        }


        [WebMethod(Description = "QRY donde se obtiene los Devoluciones")]
        public string ConsultarDevoluciones_X_Articulos_Fecha(string SucursalID, string ArticulosID, string FechaInicio, string FechaFin)
        {
            string Query;
            System.Data.DataSet ds;
            System.Xml.XmlElement xmlElement;
            Query = @"SELECT 
                        SUM (CortesYDevoluciones.Cantidad) AS Cantidad,
	                    SUM(CortesYDevoluciones.CostoSIVA)  AS CostoSIva,
	                    SUM(CortesYDevoluciones.IVA)AS Iva,
	                    SUM(CortesYDevoluciones.Total) AS Total,
	                    SUM(CortesYDevoluciones.TotalSinIVA) AS TotalSinIVA,
   	                    CortesYDevoluciones.ArticulosID AS ArticulosID
                    FROM 
	                    CortesY,	
	                    CortesYDevoluciones
                    WHERE 
	                    CortesY.CortesYID = CortesYDevoluciones.CortesYID
	                    AND
	                    (
		                    CortesY.Fecha BETWEEN '" + FechaInicio + "' AND '" + FechaFin + @"'
		                    AND	CortesYDevoluciones.ArticulosID IN (" + ArticulosID + @") 
		                    AND	CortesY.SucursalesID = " + SucursalID + @"
	                    )
	                    GROUP BY
	                    CortesYDevoluciones.ArticulosID";
            try
            {
                ds = qryToDataSet(Query);
                if (ds.Tables.Count > 0)
                {
                    xmlElement = Serialize(ds.Tables[0]);
                    return xmlElement.OuterXml.ToString();
                }
                return "";
            }
            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "ConsultarDevoluciones_X_Articulos_Fecha:" + ex.Message + ex.StackTrace + "\n" + Query);
                return "Ocurrio un error inesperado";
            }
        }



        [WebMethod(Description = "QRY donde se obtiene los Devoluciones del proveedor")]
        public string ConsultarDevolucionesProveedor_X_ListaArticulos(string SucursalID, string ArticulosID, string FechaInicio, string FechaFin, string TipoMovimientoID)
        {
            string Query;
            System.Data.DataSet ds;
            System.Xml.XmlElement xmlElement;
            Query = @"SELECT 
	                    Salidas.SalidasID AS SalidasID,	
	                    Salidas.DestinosID AS DestinosID,	
	                    Salidas.TiposMovimientosID AS TiposMovimientosID,	
	                    Salidas.SucursalesID AS SucursalesID,	
	                    Salidas.ProveedoresID AS ProveedoresID,	
	                    Salidas.Estatus_MovimientosID AS Estatus_MovimientosID,	
	                    Salidas.EmpleadoGeneroID AS EmpleadoGeneroID,	
	                    Salidas.EmpleadoRecibioID AS EmpleadoRecibioID,	
	                    Salidas.EmpleadoDevolvioID AS EmpleadoDevolvioID,	
	                    Salidas.EmpleadoCanceloID AS EmpleadoCanceloID,	
	                    Salidas.FechaSalida AS FechaSalida,	
	                    Salidas.LineasID AS LineasID,	
	                    Salidas.CantidadSalida AS CantidadSalida,	
	                    SalidasArticulos.ArticulosID AS ArticulosID,	
	                    SalidasArticulos.Cantidad AS Cantidad,	
	                    SalidasArticulos.CostoSIVA AS CostoSIVA,	
	                    SalidasArticulos.IVA AS IVA,	
	                    SalidasArticulos.Notas AS Notas,	
	                    SalidasArticulos.ImporteCosto AS ImporteCosto,	
	                    SalidasArticulos.ImporteIVACosto AS ImporteIVACosto,	
	                    SalidasArticulos.Devuelto AS Devuelto
                    FROM 
	                    Salidas,	
	                    SalidasArticulos
                    WHERE 
	                    Salidas.SalidasID = SalidasArticulos.SalidasID
	                    AND
	                    (
		                    Salidas.FechaSalida BETWEEN '" + FechaInicio + "' AND '" + FechaFin + @"'
		                    AND	SalidasArticulos.ArticulosID IN (" + ArticulosID + @") 
		                    AND	Salidas.Estatus_MovimientosID = 1
		                    AND	Salidas.TiposMovimientosID = " + TipoMovimientoID + @"
		                    AND	Salidas.SucursalesID = " + SucursalID + @"
	                    )";
            try
            {
                ds = qryToDataSet(Query);
                if (ds.Tables.Count > 0)
                {
                    xmlElement = Serialize(ds.Tables[0]);
                    return xmlElement.OuterXml.ToString();
                }
                return "";
            }
            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "ConsultarDevolucionesProveedor_X_ListaArticulos:" + ex.Message + ex.StackTrace + "\n" + Query);
                return "Ocurrio un error inesperado";
            }
        }




        [WebMethod(Description = "QRY_Entradas_Ventas_Proveedor")]
        public string QRY_Entradas_Ventas_Proveedor(string SucursalID, string ArticulosID, string FechaInicio, string FechaFin, string TipoMovimientoID, string EstatusMovimientosID)
        {
            string Query;
            System.Data.DataSet ds;
            System.Xml.XmlElement xmlElement;
            Query = @"SELECT 
	                    EntradasArticulos.ArticulosID AS ArticulosID,	
	                    SUM(EntradasArticulos.CantidadRecibida) AS sum_CantidadRecibida,	
	                    SUM(EntradasArticulos.CantidadBonificacionRecibida) AS sum_CantidadBonificacionRecibida,	
	                    SUM(EntradasArticulos.ImporteCosto) AS sum_ImporteCosto,	
	                    SUM(EntradasArticulos.ImporteIVACosto) AS sum_ImporteIVACosto,	
	                    SUM(EntradasArticulos.Cantidad) AS sum_Cantidad,	
	                    SUM(EntradasArticulos.CantidadBonificacion) AS sum_CantidadBonificacion
                    FROM 
	                    Entradas,	
	                    EntradasArticulos
                    WHERE 
	                    Entradas.EntradasID = EntradasArticulos.EntradasID
	                    AND
	                    (
		                    Entradas.FechaEntrada BETWEEN '" + FechaInicio + "' AND '" + FechaFin + @"'
		                    AND	Entradas.TiposMovimientosID IN (" + TipoMovimientoID + @") 
		                    AND	Entradas.Estatus_MovimientosID = " + EstatusMovimientosID + @"
		                    AND	Entradas.SucursalesID = " + SucursalID + @"
		                    AND	EntradasArticulos.ArticulosID IN (" + ArticulosID + @") 
	                    )
                    GROUP BY 
	                    EntradasArticulos.ArticulosID";
            try
            {
                ds = qryToDataSet(Query);
                if (ds.Tables.Count > 0)
                {
                    xmlElement = Serialize(ds.Tables[0]);
                    return xmlElement.OuterXml.ToString();
                }
                return "";
            }
            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "QRY_Entradas_Ventas_Proveedor:" + ex.Message + ex.StackTrace + "\n" + Query);
                return "Ocurrio un error inesperado";
            }
        }


        [WebMethod(Description = "QRY_NotasCredito")]
        public string QRY_NotasCredito(string SucursalID, string ArticulosID, string FechaInicio, string FechaFin, string RestaAInventario, string EstatusMovimientosID)
        {
            string Query;
            System.Data.DataSet ds;
            System.Xml.XmlElement xmlElement;
            Query = @"SELECT 
                        NotasCreditoArticulos.ArticulosID AS ArticulosID,	
                        SUM(NotasCreditoArticulos.CantidadAfectacion) AS sum_CantidadAfectacion,	
                        SUM(( NotasCreditoArticulos.CantidadAfectacion * NotasCreditoArticulos.CostoCIVA ) ) AS sum_TotalAfectacionCIVA,	
                        SUM(NotasCreditoArticulos.TotalAfectacion) AS sum_TotalAfectacion,	
                        ConceptosNotasCredito.RestaAInventario AS RestaAInventario
                     FROM 
                        ConceptosNotasCredito,	
                        NotasCreditoArticulos,	
                        NotasCredito
                     WHERE 
                        NotasCredito.NotasCreditoID = NotasCreditoArticulos.NotasCreditoID
                        AND		ConceptosNotasCredito.ConceptosNotasCreditoID = NotasCreditoArticulos.ConceptosNotasCreditoID
                        AND
                        (
                        NotasCredito.Estatus_MovimientosID = " + EstatusMovimientosID + @"
                        AND	NotasCreditoArticulos.ArticulosID IN (" + ArticulosID + @") 
                        AND	NotasCredito.FechaHoraCreacion BETWEEN '" + FechaInicio + "' AND '" + FechaFin + @"'
                        AND	NotasCredito.SucursalesID = " + SucursalID + @"
                        AND	ConceptosNotasCredito.RestaAInventario = " + RestaAInventario + @"
                        )
                     GROUP BY 
                        NotasCreditoArticulos.ArticulosID,	
                        ConceptosNotasCredito.RestaAInventario";
            try
            {
                ds = qryToDataSet(Query);
                if (ds.Tables.Count > 0)
                {
                    xmlElement = Serialize(ds.Tables[0]);
                    return xmlElement.OuterXml.ToString();
                }
                return "";
            }
            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "QRY_HistorialCambios_X_ArticuloID_X_FechaInicioFin:" + ex.Message + ex.StackTrace + "\n" + Query);
                return "Ocurrio un error inesperado";
            }
        }


        [WebMethod(Description = "QRY_HistorialCambios_X_ArticuloID_X_FechaInicioFin")]
        public string QRY_HistorialCambios_X_ArticuloID_X_FechaInicioFin(string SucursalID, string CodigoArticulo, string FechaInicio, string FechaFin, string Nivel)
        {
            string Query;
            System.Data.DataSet ds;
            System.Xml.XmlElement xmlElement;
            Query = @"SELECT 
	                    HistorialCambios.ArticulosID AS ArticulosID,	
	                    HistorialCambios.Nivel AS Nivel,	
	                    Articulos.Nombre AS Nombre,	
	                    HistorialCambios.SucursalesID AS SucursalesID,	
	                    HistorialCambios.FechaHora AS FechaHora,	
	                    HistorialCambios.PorSistema AS PorSistema,	
	                    Articulos.FamiliasID AS FamiliasID,	
	                    Familias.NombreFamilia AS NombreFamilia,	
	                    Articulos.Codigo AS Codigo,	
	                    HistorialCambios.NuevoCostoCIVA AS NuevoCostoCIVA,	
	                    HistorialCambios.NuevoPrecioCIVA AS NuevoPrecioCIVA,	
	                    HistorialCambios.CostoCIVA AS CostoCIVA,	
	                    HistorialCambios.PrecioCIVA AS PrecioCIVA,	
	                    HistorialCambios.EmpleadosID AS EmpleadosID,	
	                    HistorialCambios.Motivo AS Motivo,	
	                    Empleados.NombreCompleto AS NombreCompleto,	
	                    Lineas.NombreLinea AS NombreLinea
                    FROM 
	                    Lineas
	                    INNER JOIN
	                    (
		                    Familias
		                    INNER JOIN
		                    (
			                    Articulos
			                    INNER JOIN
			                    (
				                    Empleados
				                    RIGHT OUTER JOIN
				                    HistorialCambios
				                    ON Empleados.EmpleadosID = HistorialCambios.EmpleadosID
			                    )
			                    ON Articulos.ArticulosID = HistorialCambios.ArticulosID
		                    )
		                    ON Familias.FamiliasID = Articulos.FamiliasID
	                    )
	                    ON Lineas.LineasID = Familias.LineasID
                    WHERE 
	                    (

	                    HistorialCambios.FechaHora BETWEEN '" + FechaInicio + "' AND '" + FechaFin + @"'
	                    AND	Articulos.Codigo = '" + CodigoArticulo + @"' 
	                    AND	HistorialCambios.SucursalesID = " + SucursalID + @"             
	                    AND	HistorialCambios.Nivel = '" + Nivel + @"'
	              
                    )
                    ORDER BY 
	                    FechaHora DESC";
            try
            {
                ds = qryToDataSet(Query);
                if (ds.Tables.Count > 0)
                {
                    xmlElement = Serialize(ds.Tables[0]);
                    return xmlElement.OuterXml.ToString();
                }
                return "";
            }
            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "QRY_HistorialCambios_X_ArticuloID_X_FechaInicioFin:" + ex.Message + ex.StackTrace + "\n" + Query);
                return "Ocurrio un error inesperado";
            }
        }



        [WebMethod(Description = "QRY_ArticulosPreciosCostos_X_ArticulosID_SucursalesID_Nivel")]
        public string QRY_ArticulosPreciosCostos_X_ArticulosID_SucursalesID_Nivel(string SucursalID, string ArticulosID, string Nivel)
        {
            string Query;
            System.Data.DataSet ds;
            System.Xml.XmlElement xmlElement;
            Query = @"SELECT 
	                    ArticulosPrecios.ArticulosPreciosID AS ArticulosPreciosID,	
	                    ArticulosPrecios.ArticulosID AS ArticulosID,	
	                    ArticulosPrecios.SucursalesID AS SucursalesID,	
	                    ArticulosPrecios.Nivel AS Nivel,	
	                    ArticulosPrecios.Precio AS Precio,	
	                    ArticulosPrecios.PrecioCIVA AS PrecioCIVA,	
	                    ArticulosPrecios.Costo AS Costo,	
	                    ArticulosPrecios.CostoCIVA AS CostoCIVA,	
	                    ArticulosPrecios.PrecioAnterior AS PrecioAnterior,	
	                    ArticulosPrecios.CostoAnterior AS CostoAnterior,	
	                    ArticulosPrecios.Activo AS Activo,
	                    ArticulosPrecios.InternalVersion AS InternalVersion
                    FROM 
	                    ArticulosPrecios
                    WHERE 
	                    ArticulosPrecios.SucursalesID = " + SucursalID + @"  
	                    AND	ArticulosPrecios.Nivel = '" + Nivel + @"'  
	                    AND	ArticulosPrecios.ArticulosID IN (" + ArticulosID + @")";
            try
            {
                ds = qryToDataSet(Query);
                if (ds.Tables.Count > 0)
                {
                    xmlElement = Serialize(ds.Tables[0]);
                    return xmlElement.OuterXml.ToString();
                }
                return "";
            }
            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "QRY_ArticulosPreciosCostos_X_ArticulosID_SucursalesID_Nivel:" + ex.Message + ex.StackTrace + "\n" + Query);
                return "Ocurrio un error inesperado";
            }
        }


        [WebMethod(Description = "Consutar costos articulos X listado ArticulosID")]
        public string Consultar_HistorialCambios_X_ArticulosID(string SucursalID, string ArticulosID, string FechaInicio, string FechaFin, string Nivel)
        {
            string Query;
            System.Data.DataSet ds;
            System.Xml.XmlElement xmlElement;
            Query = @"select 
	                    HistorialCambios.ArticulosID,
	                    max(HistorialCambios.FechaHora) AS FechaHora,
	                    HistorialCambios.NuevoCostoCIVA AS NuevoCostoCIVA	
                    from
	                    HistorialCambios
                    where
	                    HistorialCambios.ArticulosID IN (" + ArticulosID + @") 
	                    and	HistorialCambios.Nivel = '" + Nivel + @"'  
	                    and	HistorialCambios.FechaHora BETWEEN '" + FechaInicio + "' AND '" + FechaFin + @"'
	                    and  HistorialCambios.SucursalesID = " + SucursalID + @"  
                    GROUP by 
	                    ArticulosID,
	                    NuevoCostoCIVA
                    ORDER BY 
                        ArticulosID,
                        FechaHora DESC";
            try
            {
                ds = qryToDataSet(Query);
                if (ds.Tables.Count > 0)
                {
                    xmlElement = Serialize(ds.Tables[0]);
                    return xmlElement.OuterXml.ToString();
                }
                return "";
            }
            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "Consultar_HistorialCambios_X_ArticulosID:" + ex.Message + ex.StackTrace + "\n" + Query);
                return "Ocurrio un error inesperado";
            }
        }


        [WebMethod(Description = "Rastreo de Articulos conteos lineas")]
        public string RastreoArticulos_ConteosLinea(string ArticulosID, string FechaInicial, string FechaFinal)
        {
            string Query;
            System.Data.DataSet ds;
            System.Xml.XmlElement xmlElement;
            Query = @"select DISTINCT
                        ConteosArticulos.ArticulosID,
	                    sum(ConteosArticulos.Diferencia) as ResultadoInventario,
	                    avg(ConteosArticulos.CostoConIva) as CostoConIVA,
	                    sum(ConteosArticulos.Diferencia*ConteosArticulos.CostoConIva) as ImporteAfectacion
                    from
	                    conteos,
	                    ConteosArticulos
                    where   
                        conteos.ConteosID = ConteosArticulos.ConteosID         	                   
                        and ConteosArticulos.ArticulosID  IN (" + ArticulosID + @") 
                        and Conteos.Fecha BETWEEN '" + FechaInicial + "' and '" + FechaFinal + @"'
                        and Conteos.EsBorrador = 0
                    GROUP by
                    ArticulosID";
            try
            {
                ds = qryToDataSet(Query);
                if (ds.Tables.Count > 0)
                {
                    xmlElement = Serialize(ds.Tables[0]);
                    return xmlElement.OuterXml.ToString();
                }
                return "";
            }
            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "RastreoArticulos_ConteosLinea:" + ex.Message + ex.StackTrace + "\n" + Query);
                return "Ocurrio un error inesperado";
            }
        }



        [WebMethod(Description = "RastreoArticulos_Mermas")]
        public string RastreoArticulos_Mermas(string ArticulosID, string FechaInicial, string FechaFinal, string SucursalID)
        {
            string Query;
            System.Data.DataSet ds;
            System.Xml.XmlElement xmlElement;
            Query = @"SELECT DISTINCT 
	                    Articulos.ArticulosID,	
	                    sum(SalidasArticulos.ImporteIVACosto) AS Importe,
	                    sum(SalidasArticulos.Cantidad) AS CantidadSalida
                    FROM 
	                    Salidas,
	                    SalidasArticulos,
	                    Articulos
                    WHERE 
	                    Salidas.SalidasID = SalidasArticulos.SalidasID
	                    AND SalidasArticulos.ArticulosID=Articulos.ArticulosID
	                    AND SalidasArticulos.ArticulosID IN (" + ArticulosID + @") 
	                    AND	Salidas.FechaSalida BETWEEN '" + FechaInicial + "' and '" + FechaFinal + @"'
	                    AND	Salidas.TiposMovimientosID = 5001
	                    AND	Salidas.SucursalesID = " + SucursalID + @"  
	                    AND	Salidas.Estatus_MovimientosID = 1
	                    AND	Salidas.PendienteAplicarNC = 0
                    GROUP by Articulos.ArticulosID
                    ";
            try
            {
                ds = qryToDataSet(Query);
                if (ds.Tables.Count > 0)
                {
                    xmlElement = Serialize(ds.Tables[0]);
                    return xmlElement.OuterXml.ToString();
                }
                return "";
            }
            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "RastreoArticulos_Mermas:" + ex.Message + ex.StackTrace + "\n" + Query);
                return "Ocurrio un error inesperado";
            }
        }

        //------------------------[Ciclicos Gerenciales]----------------------
        [WebMethod(Description = "Obtener Articulos de un proveedor en base a ProveedorID")]
        public string Articulos_X_ProvedorID(string Activo, string ProveedorID, string Nivel, string SucursalID)
        {
            string Query;
            System.Data.DataSet ds;
            System.Xml.XmlElement xmlElement;
            Query = @"SELECT 
	                    Proveedores_Articulos.ArticulosID AS ArticulosID,
	                    Articulos.Codigo AS Codigo,	
	                    Articulos.Nombre AS Nombre,
	                    ArticulosPrecios.CostoCIVA,
	                    ArticulosPrecios.PrecioCIVA
                    FROM 
	                    Articulos,	
	                    ArticulosPrecios,
	                    Proveedores_Articulos
                    WHERE 
	                    Articulos.ArticulosID = Proveedores_Articulos.ArticulosID
	                    and ArticulosPrecios.ArticulosID = Proveedores_Articulos.ArticulosID 
	                    AND Articulos.Activo = " + Activo + @"  
	                    AND Proveedores_Articulos.ProveedoresID = " + ProveedorID + @"  
	                    and ArticulosPrecios.Activo = 1
	                    and ArticulosPrecios.Nivel = " + Nivel + @"  
	                    and ArticulosPrecios.SucursalesID = " + SucursalID;
            try
            {
                ds = qryToDataSet(Query);
                if (ds.Tables.Count > 0)
                {
                    xmlElement = Serialize(ds.Tables[0]);
                    return xmlElement.OuterXml.ToString();
                }
                return "";
            }
            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "Articulos_X_ProvedorID:" + ex.Message + ex.StackTrace + "\n" + Query);
                return "Ocurrio un error inesperado";
            }
        }


        [WebMethod(Description = "Obtener los dias que se realizaron los conteos en base a un listado de ArticulosID y rango de fechas")]
        public string Consutar_DiasConteos_X_ArticulosID(string ArticulosID, string FechaInicial, string FechaFinal)
        {
            string Query;
            System.Data.DataSet ds;
            System.Xml.XmlElement xmlElement;
            Query = @"select 
	                    Articulos.ArticulosID,
	                    conteos.fecha,
	                    Conteos.EsBorrador
                    from
	                    Articulos,
	                    conteos,
	                    ConteosArticulos
                    where
	                    Articulos.ArticulosID = ConteosArticulos.ArticulosID
	                    and conteos.ConteosID = ConteosArticulos.ConteosID
	                    and conteos.Fecha BETWEEN '" + FechaInicial + "' and '" + FechaFinal + @"'
	                    and Articulos.ArticulosID IN (" + ArticulosID + @") 
                    ";
            try
            {
                ds = qryToDataSet(Query);
                if (ds.Tables.Count > 0)
                {
                    xmlElement = Serialize(ds.Tables[0]);
                    return xmlElement.OuterXml.ToString();
                }
                return "";
            }
            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "Consutar_DiasConteos_X_ArticulosID:" + ex.Message + ex.StackTrace + "\n" + Query);
                return "Ocurrio un error inesperado";
            }
        }


        [WebMethod(Description = "Obtener articulos descatalogados de un listado de articulos")]
        public string Consutar_ArticulosDescatalogados_X_ArticulosID(string ArticulosID, string SucursalID, string Inactivo)
        {
            string Query;
            System.Data.DataSet ds;
            System.Xml.XmlElement xmlElement;
            Query = @"SELECT
	                    ArticulosDescatalogados.ArticulosID 
	                 from
	                    ArticulosDescatalogados
	                 where	               
	                    ArticulosDescatalogados.Inactivo = " + Inactivo + @"  
	                    and ArticulosDescatalogados.SucursalesID = " + SucursalID + @"  
	                    and ArticulosDescatalogados.ArticulosID IN (" + ArticulosID + @")  
                    ";
            try
            {
                ds = qryToDataSet(Query);
                if (ds.Tables.Count > 0)
                {
                    xmlElement = Serialize(ds.Tables[0]);
                    return xmlElement.OuterXml.ToString();
                }
                return "";
            }
            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "Consutar_ArticulosDescatalogados_X_ArticulosID:" + ex.Message + ex.StackTrace + "\n" + Query);
                return "Ocurrio un error inesperado";
            }
        }

        //-------------------[Revision Transferencias Salidas]---------------------
        [WebMethod(Description = "Obtener detalles de las salidas en base a parametros")]
        public string Revision_Transferencias_Salidas(string SucursalSalidaID, string SucursalEntradaID, string EstatusMovimientoID, string TiposMovimientosID, string FechaInicial, string FechaFinal)
        {
            string Query;
            System.Data.DataSet ds;
            System.Xml.XmlElement xmlElement;
            Query = @"Select
	                    Salidas.SalidasID as Folio,
	                    Salidas.FolioMovimiento as FolioMovimiento,
	                    TiposMovimientos.DescripcionMovimiento as Movimiento,
	                    Salidas.Referencia as Referencia,
	                    Destinos.Nombre as DestinoProveedor,
	                    Salidas.FechaSalida as Fecha,
	                    Salidas.Importe as Importe,
	                    Estatus_Movimientos.Nombre as Estatus,
	                    empleados.NombreCompleto as EmpleadoGenero,
	                    Salidas.Recibido as Recibido,
	                    Salidas.CantidadSalida,
	                    Salidas.Notas
                    from
	                    Salidas
	                    inner JOIN TiposMovimientos on salidas.TiposMovimientosID = TiposMovimientos.TiposMovimientosID
	                    inner join Destinos on Salidas.DestinosID = Destinos.DestinosID
	                    inner join Estatus_Movimientos on Salidas.Estatus_MovimientosID = Estatus_Movimientos.Estatus_MovimientosID
	                    inner join Empleados on Salidas.EmpleadoGeneroID = Empleados.EmpleadosID
                    where
	                    Destinos.SucursalesID = " + SucursalEntradaID + @"  
                        and Salidas.SucursalesID = " + SucursalSalidaID;
            if (EstatusMovimientoID != "NULL")
            {
                Query += "and salidas.Estatus_MovimientosID = " + EstatusMovimientoID;
            }
            Query += @"
                        and Salidas.FechaSalida BETWEEN '" + FechaInicial + "' and '" + FechaFinal + @"'
	                    and Salidas.TiposMovimientosID = " + TiposMovimientosID + @"  
                    order by
	                    Salidas.FechaSalida asc
                    ";
            try
            {
                ds = qryToDataSet(Query);
                if (ds.Tables.Count > 0)
                {
                    xmlElement = Serialize(ds.Tables[0]);
                    return xmlElement.OuterXml.ToString();
                }
                return "";
            }
            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "Revision_Transferencias_Salidas:" + ex.Message + ex.StackTrace + "\n" + Query);
                return "Ocurrio un error inesperado";
            }
        }

        //----------------[FEIN: Factura Global]----------------

        [WebMethod(Description = "QRY_TotalFacturado_x_VentaID")]
        public string QRY_TotalFacturado_x_VentasID(string VentasID)
        {
            string Query;
            System.Data.DataSet ds;
            System.Xml.XmlElement xmlElement;
            Query = @"select 
	                    sum(ventas.TotalVenta)
                    from
	                    ventas
                    where
	                    Ventas.Cancelada = 0
	                    and Ventas.VentasID IN (" + VentasID + ")";
            try
            {
                ds = qryToDataSet(Query);
                if (ds.Tables.Count > 0)
                {
                    xmlElement = Serialize(ds.Tables[0]);
                    return xmlElement.OuterXml.ToString();
                }
                return "";
            }
            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "QRY_TotalFacturado_x_VentasID:" + ex.Message + ex.StackTrace + "\n" + Query);
                return "Ocurrio un error inesperado";
            }
        }

        //----------------[Rastreo de Mercancia Proveedor]----------------
        [WebMethod(Description = "QRY_Proveedores_Articulos_RPT_Proveedores_X_Activo_descatalogados")]
        public string QRY_Proveedores_Articulos_RPT_Proveedores_X_Activo_descatalogados(string ProveedoresID, string Activo, string Descatalogado)
        {
            string Query;
            System.Data.DataSet ds;
            System.Xml.XmlElement xmlElement;
            Query = @"SELECT DISTINCT 
	                    Proveedores_Articulos.ArticulosID AS ArticulosID,	
	                    Articulos.Codigo AS Codigo,	
	                    Articulos.Nombre AS Nombre
                    FROM 
	                    Articulos,	
	                    Proveedores_Articulos
                    WHERE 
	                    Articulos.ArticulosID = Proveedores_Articulos.ArticulosID
	                    AND
	                    (
		                    Proveedores_Articulos.ProveedoresID = " + ProveedoresID + @" 
		                    AND	Articulos.Activo = " + Activo;
            if (Descatalogado != "2")
            {
                Query += " AND Articulos.Descatalogado = " + Descatalogado;
            }
            Query += @" )
                    ORDER BY 
	                    Nombre ASC";
            try
            {
                ds = qryToDataSet(Query);
                if (ds.Tables.Count > 0)
                {
                    xmlElement = Serialize(ds.Tables[0]);
                    return xmlElement.OuterXml.ToString();
                }
                return "";
            }
            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "QRY_Proveedores_Articulos_RPT_Proveedores_X_Activo_descatalogados:" + ex.Message + ex.StackTrace + "\n" + Query);
                return "Ocurrio un error inesperado";
            }
        }

        //-----[Reproducir Sonido .wav]------
        [WebMethod(Description = "Reproducir sonido .wav")]
        public string Play_Sound()
        {
            try
            {
                System.Media.SoundPlayer simpleSound = new SoundPlayer(@"C:\inetpub\Ganador.wav");
                simpleSound.Play();
                return "1";
            }
            catch (Exception ex)
            {
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "Reproducir sonido .wav:" + ex.Message + ex.StackTrace);
                return "0";
            }
        }


    }
}
