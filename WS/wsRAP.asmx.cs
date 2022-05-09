using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Services;

namespace WS
{
    /// <summary>
    /// Summary description for wsRAP
    /// </summary>
    [WebService(Namespace = "http://tempuri.org/")]
    [WebServiceBinding(ConformsTo = WsiProfiles.BasicProfile1_1)]
    [System.ComponentModel.ToolboxItem(false)]
    // To allow this Web Service to be called from script, using ASP.NET AJAX, uncomment the following line. 
    // [System.Web.Script.Services.ScriptService]
    public class wsRAP : System.Web.Services.WebService
    {
        [WebMethod]
        public string HelloWorld(String sNombre)
        {
            return "Hello World "+ sNombre;
        }
   

    private System.Data.DataSet qryToDataSet(String qry)
    {
        System.Data.Odbc.OdbcConnection MyConnection = new System.Data.Odbc.OdbcConnection("DSN=HyperFileRAP");
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

        //---------------------[Obtener Envios RAP]--------------------------

        [WebMethod(Description = "Obtener Envios RAP")]
        public string ObtenerEnviosRAP(String FechaInicial, String FechaFinal)
        {
            String sQry = @"SELECT DISTINCT    
                                Envios.IDEnvios AS EnvioID,
                                Envios.CAT_FiguraTransporteID AS ChoferesID,
                                Envios.VehiculoID AS VehiculoID,
                                Envios.IDRemolques AS RemolqueID,
                                Envios.PesoBrutoTotal AS PesoBrutoTotal,
                                Envios.PesoNetoTotal AS PesoNetoTotal,                                                        
	                            Envios.Folio AS Folio,	
	                            CONCAT(Tipo_Vehiculo.Descripcion, ' ', Marca.Descripcion, ' ', Modelo.Descripcion) AS Vehiculo,
                                CONCAT(CAT_FiguraTransporte.Nombre, ' ', CAT_FiguraTransporte.ApellidoPaterno, ' ', CAT_FiguraTransporte.ApellidoMaterno) AS NombreChofer,
                                Usuario.Nombre_Completo AS UsuarioGeneroNombre,
	                            Envios.Fecha_Hora_Generacion AS FechaHoraGeneracion,
	                            Envios.Facturado AS Facturado
                            FROM Envios
                                INNER JOIN Vehiculo ON Envios.VehiculoID = Vehiculo.VehiculoID
                                INNER JOIN CAT_FiguraTransporte ON Envios.CAT_FiguraTransporteID = CAT_FiguraTransporte.CAT_FiguraTransporteID
                                INNER JOIN Marca ON Vehiculo.MarcaID = Marca.MarcaID
                                INNER JOIN Modelo ON Vehiculo.ModeloID = Modelo.ModeloID
                                INNER JOIN Tipo_Vehiculo ON Vehiculo.Tipo_VehiculoID = Tipo_Vehiculo.Tipo_VehiculoID
                                INNER JOIN Usuario ON Envios.UsuarioID_Genero = Usuario.UsuarioID
                            WHERE
                                Envios.Facturado = 0
                                AND Envios.Activo = 1
                                AND Envios.Fecha_Hora_Generacion BETWEEN '" + FechaInicial + "' AND '" + FechaFinal + @"'
                            ORDER BY Envios.Fecha_Hora_Generacion DESC";

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
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "Obtener Envios RAP:" + ex.Message + ex.StackTrace + "\n" + sQry);
                return "Ocurrio un error inesperado";
            }
        }

        //---------------------[Obtener Ruta de Envio RAP]--------------------------

        [WebMethod(Description = "Obtener Ruta de Envio RAP")]
        public string ObtenerRutaEnvioRAP(String EnvioID)
        {
            String sQry = @"SELECT
                                Envio_Ruta.Origen AS OrigenID,
                                Envio_Ruta.Destino AS DestinoID,
                                Envio_Ruta.Orden AS Orden,
                                Envio_Ruta.Distancia AS Distancia,
                                Envio_Ruta.FechaHoraSalidaEstimada AS FechaHoraSalida,
                                Envio_Ruta.FechaHoraLlegadaEstimada AS FechaHoraLlegada
                            FROM 
                                Envio_Ruta
                            WHERE
                                Envio_Ruta.IDEnvios=" + EnvioID +
                                @"AND Envio_Ruta.Orden > 0 ";

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
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "Obtener Envios Obtener Ruta RAP:" + ex.Message + ex.StackTrace + "\n" + sQry);
                return "Ocurrio un error inesperado";
            }
        }

        //---------------------[Obtener Direcciones Envio RAP]--------------------------
        [WebMethod(Description = "Obtener Direcciones Envio RAP")]
        public string ObtenerDireccionesEnvioRAP(String EnvioID)
        {
            String sQry = @"SELECT
	                            Direcciones.IDDirecciones AS DireccionesID,
	                            Direcciones.RFC AS RFC,
	                            Direcciones.Nombre AS Nombre,
	                            Direcciones.CodigoPostal as CodigoPostal,
	                            Direcciones.Calle as Calle,
	                            Direcciones.NumeroExterior as NumeroExterior,
	                            Direcciones.NumeroInterior as NumeroInterior,
	                            Direcciones.CodigoColonia as CodigoColonia,
	                            Direcciones.Colonia as ColoniaDrescripcion,
	                            Direcciones.CodigoLocalidad as CodigoLocalidad,
	                            Direcciones.Localidad as LocalidadDescripcion,
	                            Direcciones.Referencia as Referencia,
	                            Direcciones.CodigoMunicipio as CodigoMunicipio,
	                            Direcciones.Municipio as MunicipioDescipcion,
	                            Direcciones.CodigoEstado as CodigoEstado,
	                            Direcciones.CodigoPais as CodigoPais
                            FROM
	                            Direcciones
                            WHERE
	                            Direcciones.IDDirecciones IN 
	                            (SELECT Envio_Ruta.Origen FROM Envio_Ruta
	                            WHERE Envio_Ruta.IDEnvios=" + EnvioID +
                          @"UNION
	                            SELECT Envio_Ruta.Destino from Envio_Ruta
	                            where Envio_Ruta.IDEnvios="+ EnvioID+")";

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
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "Obtener Direcciones Envio RAP: " + ex.Message + ex.StackTrace + "\n" + sQry);
                return "Ocurrio un error inesperado";
            }
        }


        //---------------------[Obtener Articulo Envio RAP]--------------------------

        [WebMethod(Description = "Obtener Articulos Envio RAP")]
        public string ObtenerArticulosEnvioRAP(String EnvioID)
        {
            String sQry = @"SELECT DISTINCT
	                            Envios_Detalles_Articulos.ClaveSAT as ArticuloCodigo,
	                            Envios_Detalles_Articulos.Descripcion AS ArticuloDescripcion,	
	                            Envios_Detalles_Articulos.Cantidad AS Cantidad,
	                            Envios_Detalles_Articulos.UnidadSAT AS UnidadCodigo,
	                            Envios_Detalles_Articulos.EsPeligroso AS EsMaterialPeligroso,	
	                            MaterialPeligroso.Clave AS MaterialPeligrosoCodigo,
	                            TipoEmbalaje.Clave AS TipoEmbalajeCodigo,
	                            TipoEmbalaje.Descripcion AS TipoEmbalaje,
	                            Envios_Detalles_Articulos.PesoNeto AS PesoEnKilogramos,
	                            Envios_Detalles_Articulos.Importe AS ValorMercancia,
	                            'MXN' as Moneda,
	                            Envios_Detalles_Articulos.PesoBruto AS PesoBruto,
	                            Envios_Detalles_Articulos.PesoNeto AS PesoNeto,
	                            (Envios_Detalles_Articulos.PesoBruto-Envios_Detalles_Articulos.PesoNeto) as PesoTara,
	                            Envio_Ruta.Orden as Orden
                            FROM
	                            Envios_Detalles_Articulos
	                            INNER JOIN Envios_Detalles ON Envios_Detalles_Articulos.IDEnvios_Detalles = Envios_Detalles.IDEnvios_Detalles
	                            left JOIN MaterialPeligroso ON Envios_Detalles_Articulos.IDMaterialPeligroso = MaterialPeligroso.IDMaterialPeligroso
	                            left JOIN TipoEmbalaje ON Envios_Detalles_Articulos.IDTipoEmbalaje = TipoEmbalaje.IDTipoEmbalaje
	                            inner join Envio_Ruta on Envio_Ruta.Destino = Envios_Detalles.IDDestino
                            WHERE
	                            Envios_Detalles.IDEnvios=" + EnvioID +
                          @"    AND
                                Envios_Detalles.IDEnvios=Envio_Ruta.IDEnvios
                                ORDER BY
	                            Envio_Ruta.Orden ASC";

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
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "Obtener Envios Obtener Ruta RAP:" + ex.Message + ex.StackTrace + "\n" + sQry);
                return "Ocurrio un error inesperado";
            }
        }

        //---------------------[Obtener Datos Transporte Vehiculo]--------------------------

        [WebMethod(Description = "Obtener Datos Transporte Vehiculo RAP")]
        public string ObtenerDatosTransporteVehiculo(String VehiculoID)
        {
            String sQry = @"SELECT
	                            Vehiculo.Matricula as VehiculoMatricula,
	                            Vehiculo.Anio AS ModeloVehiculo,
							    CAT_FiguraTransporte.RFC as RFC_RegistroSAT,
							    CATSAT_ParteTransporte.Clave as ClaveParteTransporte,		
	                            Permiso.Clave AS ClavePermisoSCT,
	                            Vehiculo_Datos_SAT.NumPermisoSCT as NumeroPermiso,
	                            Configuracion_Vehicular_SAT.ClaveNomeclatura as ConfiguracionVehicular,
	                            Tipo_Aseguradora.Descripcion as TipoSeguro, 
	                            Aseguradora.Nombre AS NombreAseguradora,
	                            Polizas_Datos_SAT.Numero_de_Poliza as NumeroPoliza,
                                Vehiculo.CAT_FiguraTransporteID as FiguraTransporteID
                            FROM Vehiculo_Datos_SAT
	                            inner join Vehiculo on Vehiculo_Datos_SAT.VehiculoID = Vehiculo.VehiculoID 	
	                            inner join Configuracion_Vehicular_SAT on Vehiculo_Datos_SAT.Configuracion_Vehicular_SATID=Configuracion_Vehicular_SAT.Configuracion_Vehicular_SATID
	                            inner JOIN permiso ON Vehiculo_Datos_SAT.IDPermiso =  permiso.IDPermiso
	                            inner join Polizas_Datos_SAT on Vehiculo_Datos_SAT.Vehiculo_Datos_SATID=Polizas_Datos_SAT.Vehiculo_Datos_SATID
	                            INNER JOIN Polizas ON Polizas_Datos_SAT.PolizasID = Polizas.PolizasID
	                            INNER JOIN Aseguradora ON Polizas.AseguradoraID = Aseguradora.AseguradoraID
	                            inner join Tipo_Aseguradora on Polizas.IDTipo_Aseguradora=Tipo_Aseguradora.IDTipo_Aseguradora
							    inner join CAT_FiguraTransporte on Vehiculo.CAT_FiguraTransporteID = CAT_FiguraTransporte.CAT_FiguraTransporteID
							    inner join CATSAT_ParteTransporte on Vehiculo.CATSAT_ParteTransporteID=CATSAT_ParteTransporte.CATSAT_ParteTransporteID
                            WHERE 
	                            vehiculo.VehiculoID=" + VehiculoID;

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
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "Obtener Articulos Envio RAP:" + ex.Message + ex.StackTrace + "\n" + sQry);
                return "Ocurrio un error inesperado";
            }
        }

        //---------------------[Obtener Datos Transporte Remolque]--------------------------

        [WebMethod(Description = "Obtener Articulos Envio RAP")]
        public string ObtenerDatosTransporteRemolque(String RemolqueID)
        {
            String sQry = @"SELECT
	                            Tipos_Remolques.Clave as ClaveRemolque,
	                            remolques.Placas as PlacasRemolque,
	                            CAT_FiguraTransporte.RFC as RFC_RegistroSAT,
	                            CATSAT_ParteTransporte.Clave as ClaveParteTransporte,
                                remolques.CAT_FiguraTransporteID as FiguraTransporteID		
                            from remolques
	                            inner JOIN Tipos_Remolques on Tipos_Remolques.IDTipos_Remolques=remolques.IDTipos_Remolques
	                            inner join CAT_FiguraTransporte on remolques.CAT_FiguraTransporteID = CAT_FiguraTransporte.CAT_FiguraTransporteID
	                            inner join CATSAT_ParteTransporte on remolques.CATSAT_ParteTransporteID=CATSAT_ParteTransporte.CATSAT_ParteTransporteID
                            where
	                            remolques.IDRemolques=" + RemolqueID;
                                     
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
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "Obtener Datos Transporte Remolque:" + ex.Message + ex.StackTrace + "\n" + sQry);
                return "Ocurrio un error inesperado";
            }
        }


        //---------------------[Obtener Datos Figura Transporte]--------------------------

        [WebMethod(Description = "Obtener Figura Transporte RAP")]
        public string ObtenerDatosFiguraTransporte(String Operador_Vehiculo_Remolque)
        {
            String sQry = @"SELECT                              
	                            CAT_FiguraTransporte.CAT_FiguraTransporteID as FiguraTransporteID,
                                CONCAT(CATSAT_FiguraTransporte.Clave,'-',CATSAT_FiguraTransporte.Descripcion) as TipoFiguraTransporte,
	                            CAT_FiguraTransporte.RFC as RFCFiguraTransporte,
	                            CAT_FiguraTransporte.NumeroLicencia as NumeroLicencia,
	                            CONCAT(CAT_FiguraTransporte.Nombre,' ',CAT_FiguraTransporte.ApellidoPaterno,' ',CAT_FiguraTransporte.ApellidoMaterno) as NombreFigura,
	                            CAT_FiguraTransporte.NRI_Extrangero as NumeroIdentificacionFiguraTransporte,
	                            CAT_FiguraTransporte.CodigoPais as ResidenciaFiscal,
	                            CAT_FiguraTransporte.Calle as Calle,
	                            CAT_FiguraTransporte.NumeroExterior as NumeroExterior,
	                            CAT_FiguraTransporte.NumeroInterior as NumeroInterior,
	                            CONCAT(CAT_FiguraTransporte.CodigoColonia,'-',CAT_FiguraTransporte.Colonia) as Colonia,
	                            CONCAT(CAT_FiguraTransporte.CodigoLocalidad,'-',CAT_FiguraTransporte.Localidad) as Localidad,
	                            CAT_FiguraTransporte.Referencia as Referencia,
	                            CONCAT(CAT_FiguraTransporte.CodigoMunicipio,'-',CAT_FiguraTransporte.Municipio) as Municipio,
	                            CONCAT(CAT_FiguraTransporte.CodigoEstado,'-',CAT_FiguraTransporte.Estado) as Estado,
	                            CONCAT(CAT_FiguraTransporte.CodigoPais,'-',CAT_FiguraTransporte.Pais) as Pais,
	                            CAT_FiguraTransporte.CodigoPostal as CodigoPostal
                            from
	                            CAT_FiguraTransporte
	                            inner join CATSAT_FiguraTransporte on CAT_FiguraTransporte.CATSAT_FiguraTransporteID=CATSAT_FiguraTransporte.CATSAT_FiguraTransporteID
                            where
	                            CAT_FiguraTransporte.CAT_FiguraTransporteID in (" + Operador_Vehiculo_Remolque+ @")";

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
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "ObtenerDatosFiguraTransporte:" + ex.Message + ex.StackTrace + "\n" + sQry);
                return "Ocurrio un error inesperado";
            }
        }



        //---------------------[Actualizar estatus envio]--------------------------

        [WebMethod(Description = "ActualizarEnvioFacturado")]
        public string ActualizarEnvioFacturado(String EnvioID)
        {
            String sQry = @"UPDATE envios
	                           set Facturado=1
                            where
	                            Envios.IDEnvios= " + EnvioID;
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
                System.IO.File.WriteAllText(@"C:\sXML\" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".err", "ActualizarEnvioFacturado:" + ex.Message + ex.StackTrace + "\n" + sQry);
                return "Ocurrio un error inesperado";
            }
        }




    }
}