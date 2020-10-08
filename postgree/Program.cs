using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using System.Runtime.CompilerServices;

using ETM.WCCOA;

using Npgsql;
using static WCCOApostgree.Utils;

namespace GettingStarted
{
    class Program
    {
        //Контейнер одной архивной записи
        public struct _offline<T> {
            public UInt32 _dpid;
            public Int32 _dpelid;
            public DateTime _stime;
            public UInt64 _status64;
            public UInt64 _userbit64;
            public Int32 _type;
            public Int32 _user;
            public Int32 _manager;
            public bool _corr;
            public string[] _text;
            public T _value;
        }
        
        public class _offline_Buffer_Base
        {
            static public uint Timeout = 3000;//Период выгрузки данных в БД, миллисекунд
            static public uint Size = 1024;//Максимальное количество записей, размещаемых в каждом буфере
            static public NpgsqlConnection conn;//Соединение с БД
            static Object[] Buffer = new Object[64];//(_offline_Buffer<T>) Перечень буферов для хранения данных
            static public OaManager OAManager;
            static public OaProcessValues valueAccess;
            static public OaProcessModel dpAccess;
            static public string TableNamePrefix;//Первая часть имён таблиц архива
            //static string[] OADPList;//Список архивируемых точек
            static public void Init(OaManager _OAManager, NpgsqlConnection _conn,string _TableNamePrefix="",  uint _Size = 1024,uint _Timeout = 3000) {
                OAManager = _OAManager;
                valueAccess = OAManager.ProcessValues;
                dpAccess = OAManager.ProcessModel;
                Size = _Size;
                Timeout = _Timeout;
                conn = _conn;
                TableNamePrefix = _TableNamePrefix;

                DBQueryTemplate = DBQueryTemplateBase.Replace("%TP", TableNamePrefix);//шаблон запроса на запись данных в таблицу БД

                //Инициализация буферов и обработчиков
                Buffer = new object[] {
                    new _offline_Buffer<bool>       ("bit"          ,"DPEL_BOOL"           ,(int)OaElementType.Bit          ,(v)=> {return (bool) v; }),
                    new _offline_Buffer<UInt32>     ("bit32"        ,"DPEL_BIT32"          ,(int)OaElementType.Bit32        ,(v)=> {return (UInt32)v; }),
                    new _offline_Buffer<UInt64>     ("bit64"        ,"DPEL_BIT64"          ,(int)OaElementType.Bit64        ,(v)=> {return (UInt64)v; }),
                    new _offline_Buffer<byte[]>     ("blob"         ,"DPEL_BLOB"           ,(int)OaElementType.Blob         ,(v)=> {return (byte[])v; }),
                    new _offline_Buffer<char>       ("char"         ,"DPEL_CHAR"           ,(int)OaElementType.Char         ,(v)=> {return (char)v; }),
                    new _offline_Buffer<UInt32>     ("dpid"         ,"DPEL_DPID"           ,(int)OaElementType.DpId         ,(v)=> {return (UInt32)v; }),
                    new _offline_Buffer<double>     ("float"        ,"DPEL_FLOAT"          ,(int)OaElementType.Float        ,(v)=> {return (double)v; }),
                    new _offline_Buffer<Int32>      ("int"          ,"DPEL_INT"            ,(int)OaElementType.Int          ,(v)=> {return (Int32)v; }),
                    new _offline_Buffer<string[]>   ("langtext"     ,"DPEL_LANGSTRING"     ,(int)OaElementType.LangText     ,(v)=> {return (string[]) v.AsEnumerable<string>(); }),
                    new _offline_Buffer<Int64>      ("long"         ,"DPEL_LONG"           ,(int)OaElementType.Long         ,(v)=> {return (Int64)v; }),
                    new _offline_Buffer<string>     ("text"         ,"DPEL_STRING"         ,(int)OaElementType.Text         ,(v)=> {return (string)v; }),
                    new _offline_Buffer<DateTime>   ("time"         ,"DPEL_TIME"           ,(int)OaElementType.Time         ,(v)=> {return (DateTime)v; }),
                    new _offline_Buffer<UInt32>     ("uint"         ,"DPEL_UINT"           ,(int)OaElementType.UInt         ,(v)=> {return (UInt32)v; }),
                    new _offline_Buffer<UInt64>     ("ulong"        ,"DPEL_ULONG"          ,(int)OaElementType.ULong        ,(v)=> {return (UInt64)v; }),
                    new _offline_Buffer<bool[]>     ("dyn_bit"      ,"DPEL_DYN_BOOL"       ,(int)OaElementType.DynBit       ,(v)=> {return (bool[])v.AsEnumerable<OaVariant>().Select(v1 => (bool)v1); }),
                    new _offline_Buffer<UInt32[]>   ("dyn_bit32"    ,"DPEL_DYN_BIT32"      ,(int)OaElementType.DynBit32     ,(v)=> {return (UInt32[])v.AsEnumerable<OaVariant>().Select(v1 => (UInt32)v1); }),
                    new _offline_Buffer<UInt64[]>   ("dyn_bit64"    ,"DPEL_DYN_BIT64"      ,(int)OaElementType.DynBit64     ,(v)=> {return (UInt64[])v.AsEnumerable<OaVariant>().Select(v1 => (UInt64)v1); }),
                    new _offline_Buffer<byte[][]>   ("dyn_blob"     ,"DPEL_DYN_BLOB"       ,(int)OaElementType.DynBlob      ,(v)=> {return (byte[][])v.AsEnumerable<byte[]>(); }),
                    new _offline_Buffer<char[]>     ("dyn_char"     ,"DPEL_DYN_CHAR"       ,(int)OaElementType.DynChar      ,(v)=> {return (char[])v.AsEnumerable<OaVariant>().Select(v1 => (char)v1); }),
                    new _offline_Buffer<UInt32[]>   ("dyn_dpid"     ,"DPEL_DYN_DPID"       ,(int)OaElementType.DynDpId      ,(v)=> {return (UInt32[])v.AsEnumerable<OaVariant>().Select(v1 => (UInt32)v1); }),
                    new _offline_Buffer<double[]>   ("dyn_float"    ,"DPEL_DYN_FLOAT"      ,(int)OaElementType.DynFloat     ,(v)=> {return (double[])v.AsEnumerable<OaVariant>().Select(v1 => (double)v1); }),
                    new _offline_Buffer<Int32[]>    ("dyn_int"      ,"DPEL_DYN_INT"        ,(int)OaElementType.DynInt       ,(v)=> {return (Int32[])v.AsEnumerable<OaVariant>().Select(v1 => (Int32)v1); }),
                    new _offline_Buffer<string[][]> ("dyn_langtext" ,"DPEL_DYN_LANGSTRING" ,(int)OaElementType.DynLangText  ,(v)=> {return (string[][])v.AsEnumerable<string[]>(); }),
                    new _offline_Buffer<Int64[]>    ("dyn_long"     ,"DPEL_DYN_LONG"       ,(int)OaElementType.DynLong      ,(v)=> {return (Int64[])v.AsEnumerable<OaVariant>().Select(v1 => (Int64)v1); }),
                    new _offline_Buffer<string[]>   ("dyn_text"     ,"DPEL_DYN_STRING"     ,(int)OaElementType.DynText      ,(v)=> {return (string[])v.AsEnumerable<OaVariant>().Select(v1 => (string)v1); }),
                    new _offline_Buffer<DateTime[]> ("dyn_time"     ,"DPEL_DYN_TIME"       ,(int)OaElementType.DynTime      ,(v)=> {return (DateTime[])v.AsEnumerable<OaVariant>().Select(v1 => (DateTime)v1); }),
                    new _offline_Buffer<UInt32[]>   ("dyn_uint"     ,"DPEL_DYN_UINT"       ,(int)OaElementType.DynUInt      ,(v)=> {return (UInt32[])v.AsEnumerable<OaVariant>().Select(v1 => (UInt32)v1); }),
                    new _offline_Buffer<UInt64[]>   ("dyn_ulong"    ,"DPEL_DYN_ULONG"      ,(int)OaElementType.DynULong     ,(v)=> {return (UInt64[])v.AsEnumerable<OaVariant>().Select(v1 => (UInt64)v1); })
                };

            }
            //Шаблон запроса на вставку данных в БД
            public static string DBQueryTemplateBase = "COPY %TP%T FROM STDIN(FORMAT BINARY)";
            public static string DBQueryTemplate;
            //Шаблон запроса на изменения данных в точках
            public static string OAQueryTemplateBase = 
                "SELECT " +
                    " '_online.._stime'," +
                    " '_online.._value'," +
                    " '_online.._status64'," +
                    " '_online.._userbits'," +
                    " '_online.._user'," +
                    " '_online.._manager'," +
                    " '_online.._text'," +
                    " '_online.._corr'" +
                " FROM '{%P}' WHERE _ELC = %T";
        }
        
        public class _offline_Buffer<T>: _offline_Buffer_Base
        {
            _offline<T>[] Buffer;
            uint BufferIdx = 0;
            Timer BufferTimer;
            public Func<OaVariant,T> GetValue;
            string DBQuery;
            //string OADPelTypeName;
            string OAQueryTemplate;
            string OAQuery;
            //Int32 OADPelTypeID;


            //Подготовить буфер и преобразователь типов OA в .Net
            public _offline_Buffer(string _DBTableName,string _OADPelTypeName,Int32 _OADPelTypeID, Func<OaVariant,T > _GetValue)
            {
                DBQuery = DBQueryTemplate.Replace("%T", _DBTableName);//запрос на запись данных в таблицу БД
                OAQueryTemplate = OAQueryTemplateBase.Replace("%T", _OADPelTypeName);//шаблон запроса на чтение данных из OA
                //OADPelTypeID = _OADPelTypeID;
                GetValue = _GetValue;
                Buffer = new _offline<T>[Size]; 
                BufferTimer = new Timer(new TimerCallback((me) => {
                    _offline<T>[] _Buffer;
                    lock (Buffer)//Предотвратить одновременный доступ к буферу
                    {
                        _Buffer = (_offline<T>[])Buffer.Clone();//Подготовить данные для сохранения и освободить буфер для заполнения
                        BufferIdx = 0;//Очистить буфер
                    }
                    //Вызвать запись в БД
                    using (var writer = conn.BeginBinaryImport(DBQuery))
                    {
                        try
                        {
                            foreach (var row in Buffer)
                            {
                                writer.StartRow();
                                writer.Write(row._dpid);
                                writer.Write(row._dpelid);
                                writer.Write(row._stime);
                                writer.Write(row._status64);
                                writer.Write(row._userbit64);
                                writer.Write(row._type);
                                writer.Write(row._user);
                                writer.Write(row._manager);
                                writer.Write(row._corr);
                                writer.Write(row._text);
                                writer.Write(row._value);
                            }
                            writer.Close();
                        }
                        catch (Exception e)
                        {
                            writer.Dispose();
                            Console.WriteLine("_offline_Buffer.Timer:",e);
                            throw;
                        }
                    }
                }),this, System.Threading.Timeout.Infinite,Timeout);

                OaDpQuerySubscription QuerySubscription = valueAccess.CreateDpQuerySubscription();
                //Определиться со списоком архивных тэгов
                var ConfigProc = valueAccess.CreateDpValueSingleSubscription();
                string ArchNum = "1";//Номер архива (всегда=1)
                ConfigProc.ValueChanged += (obj, v) =>
                {
                    //string ArchNum = v.Value.DpName.Split('=')[1];
                    //if (String.IsNullOrEmpty(ArchNum)) return;//Номер архива не задан
                    //Подготовиьт запрос к списку тэгов
                    OAQuery = OAQueryTemplate.Replace("%P", String.Join(",", v.Value.DpValue.AsEnumerable<OaDpIdentifier>().Select((id) => {return dpAccess.GetDpPathByDpIdentifier(id); })));
                    //Настройка сбора данных с OA
                    //QuerySubscription.StopAsync();
                    QuerySubscription.SetQuery(OAQuery);
                    //Обработка поступающих в архив значений
                    QuerySubscription.ValueChanged += (vcsender, vce) =>
                    {
                        if (Size==0) return;//Буфер не создан
                        if (BufferIdx >= Size) return;//Буфер заполнен сообщить в OA
                        var Result = vce.Result;
                        var RowCount = Result.GetRecordCount();
                        UInt64[] status64 = new UInt64[1];
                        UInt64[] userbit64 = new UInt64[1];
                        for (var i = 0; i < RowCount; i++) {
                            var dp = Result.GetData(i, 0).ToDpIdentifier();
                            lock (Buffer)//Предотвратить одновременный доступ к буферу
                            {
                                ((System.Collections.BitArray)Result.GetData(i, 3).ToBit64()).CopyTo(status64, 0);//Преобразовать набор бит в слово 64 разрядное
                                ((System.Collections.BitArray)Result.GetData(i, 4).ToBit64()).CopyTo(userbit64, 0);//Преобразовать набор бит в слово 64 разрядное
                                //Подготовка записи к сохранению
                                Buffer[BufferIdx++] = new _offline<T>
                                {
                                    _dpid = dp.Dp,
                                    _dpelid = dp.Element,
                                    _stime = Result.GetData(i, 1),
                                    _value = GetValue(Result.GetData(i, 2)),
                                    _status64 = status64[0],
                                    _userbit64 = userbit64[0],
                                    _type = dp.DpType,
                                    _user = Result.GetData(i, 5),
                                    _manager = Result.GetData(i, 6),
                                    _text = (string[])Result.GetData(i, 7).AsEnumerable<string>(),
                                    _corr = Result.GetData(i, 8)
                                };
                            }
                            
                            if (BufferIdx >= Size) {//Буфер заполнен сохранить в базе данных
                                BufferTimer.Change(0, Timeout);//Немедленно запустить сохранение в БД
                            }

                        }
                    };
                    QuerySubscription.StartAsync();
                };
                ConfigProc.AddDp(dpAccess.GetDpIdentifierByDpPath("PostgresProConfig="+ ArchNum + ".DPList"));
                ConfigProc.FireChangedEventForAnswer = true;
                ConfigProc.StartAsync();//Получить список тэгов и получать его изменения
            }
        }
            
        static void Main(string[] args)
        {
            // Create Manager object
            OaManager myManager = OaSdk.CreateManager();

            // Initialize Manager Configuration
            myManager.Init(ManagerSettings.DefaultApiSettings, args);

            // Start the Manager and Connect to the OA project with the given configuration
            myManager.Start();
            
            OaConfigurationFile file = new OaConfigurationFile();

            // get connection params from config
            string cfg_host  = file.ReadString("postgrees", "hostname", "localhost");
            string cfg_user  = file.ReadString("postgrees", "username", "postgres");
            string cfg_pass  = file.ReadString("postgrees", "password", "root");
            string cfg_dbase = file.ReadString("postgrees", "database", "OEM_Kaskad");
            uint BufferSize = (uint)file.ReadInt32("postgrees", "BufferSize", 1024);
            uint BufferTimeout = (uint)file.ReadInt32("postgrees", "BufferTimeout", 3000);
            string TablePrefix = file.ReadString("postgrees", "TablePrefix", "ar0_offline_value_");


            Console.WriteLine("1=============postgree manager init===============");
            Console.WriteLine("Hostname: " + cfg_host);
            Console.WriteLine("Database: " + cfg_dbase);
            Console.WriteLine("Username: " + cfg_user);
            Console.WriteLine("Password: " + cfg_pass);
            bool debug = true;
            while ( debug){
                Thread.Sleep(100);
            }
            Console.WriteLine("=================================================");

            // prepare connection string npgsql
            var connect_string = String.Format("Host={0};Username={1};Password={2};Database={3}", cfg_host, cfg_user, cfg_pass, cfg_dbase);
            NpgsqlConnection conn = new NpgsqlConnection(connect_string);
            try
            {
                conn.Open();
            }
            catch(Exception e)
            {
                Console.WriteLine("Error open connection database");
                Console.WriteLine("Connection string: " + connect_string);
                Console.WriteLine(e.Message);
                myManager.Stop();
            }

            //Инициализация буфера 
            _offline_Buffer_Base.Init(myManager, conn, TablePrefix,BufferSize, BufferTimeout);
        }
    }
}