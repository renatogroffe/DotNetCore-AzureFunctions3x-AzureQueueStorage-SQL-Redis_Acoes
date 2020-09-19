using System.Text.Json;
using Microsoft.Extensions.Configuration;
using Microsoft.Data.SqlClient;
using Dapper.Contrib.Extensions;
using StackExchange.Redis;
using FunctionAppProcessarAcoes.Models;

namespace FunctionAppProcessarAcoes.Data
{
    public class AcoesRepository
    {
        private readonly string _strConexaoBaseAcoes;
        private readonly ConnectionMultiplexer _conexaoRedis;
        private readonly string _prefixoChaveRedis;

        public AcoesRepository(IConfiguration configuration)
        {
            _strConexaoBaseAcoes = configuration["BaseAcoes"];
            _conexaoRedis = ConnectionMultiplexer.Connect(
                configuration["Redis:Connection"]);
            _prefixoChaveRedis = configuration["Redis:PrefixoChave"];
        }

        public void Save(Acao acao)
        {

            using (var conexao = new SqlConnection(_strConexaoBaseAcoes))
            {
                conexao.Insert<HistoricoAcao>(new HistoricoAcao()
                {
                    CodReferencia = $"{acao.Codigo}{acao.Data.ToString("yyyyMMddHHmmss")}",
                    Codigo = acao.Codigo,
                    DataReferencia = acao.Data,
                    Valor = acao.Valor.Value,
                    CodCorretora = acao.CodCorretora,
                    NomeCorretora = acao.NomeCorretora
                });
            }

            _conexaoRedis.GetDatabase().StringSet(
                $"{_prefixoChaveRedis}-{acao.Codigo}",
                JsonSerializer.Serialize(acao),
                expiry: null);
        }
    }
}