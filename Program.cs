using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace tasks_management_test
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var res = await test();
            Console.WriteLine($"res : {res}");
            
            var app_list = new List<string>();
            for(int i=0; i<10; i++)
            {
                app_list.Add($"app{i}");
            }
            var athena_client = new AthenaDemo();
            var query_ids = await ExecuteAllQueriesAsync(app_list, athena_client);
            await WaitUntilQueriesFinished(query_ids, athena_client);
            Console.WriteLine("All Queries are Done. Now Let's Do Other Things!");
            return;
        }

        static async Task<int> test()
        {
            var res = 0;
            await Task.Delay(5000);
            for(int i=0; i<10; i++)
            {
                Task.Delay(i * 1000);
                res++;
                Console.WriteLine(res);
            }
            return res;
        }
        static async Task<List<string>> ExecuteAllQueriesAsync(List<string> app_list, AthenaDemo athena_client)
        {
            var executing_tasks = new List<Task<string>>();

            foreach (var app_key in app_list)
            {
                executing_tasks.Add(athena_client.ExecuteQueryAsync(app_key));
                await Task.Delay(1000);
            }
            var query_ids = await Task.WhenAll(executing_tasks);
            return new List<string>(query_ids);
        }
        
        static async Task WaitUntilQueriesFinished(List<string> query_ids, AthenaDemo athena_client)
        {
            Console.WriteLine("Start Waiting!");
            var count = 0;
            var state = false;
            // 5초마다 재확인, 최대 100초까지 기다리기
            // 실제로는 내부에서 끝나지 않은 쿼리가 어떤 쿼리인지 로그 남기기
            for(int i=0; i<20; i++)
            {
                state = await CheckQueryStatesAsync(query_ids, athena_client);
                if (state == true)
                {
                    Console.WriteLine($"Finish Waiting - {count}");
                    return;
                }
                Console.WriteLine($"Queries are Not Finished Yet - {count}");
                await Task.Delay(5000);
            }
            Console.WriteLine("Finish Waiting - Some Queries Are Still Not Finished.");
        }

        static async Task<bool> CheckQueryStatesAsync(List<string> query_ids, AthenaDemo athena_client)
        {
            var checking_tasks = new List<Task<bool>>();
            // 모든 query가 끝났는지 비동기적으로 확인
            foreach (var query_id in query_ids)
            {
                checking_tasks.Add(athena_client.GetQueryStateAsync(query_id));
            }
            var states = new List<bool>(await Task.WhenAll(checking_tasks));
            // 끝나지 않은 쿼리가 하나라도 있으면 false
            if (states.Exists(query => false))
                return false;
            return true;
        }
    }

    public class AthenaDemo
    {
        private static IDictionary<string, bool> query_states = new Dictionary<string, bool>();

        public async Task<string> ExecuteQueryAsync(string app_key)
        {
            // 쿼리 리스트에 추가
            var query_id = Guid.NewGuid().ToString();
            query_states.Add(query_id, false);
            Console.WriteLine($"query start - {app_key} - {query_id}");

            // 쿼리가 끝나기까지의 1초부터 15초 사이의 랜덤 시간
            var delay_time = new Random().Next(1, 15);
            var task_delay = Task.Delay(delay_time * 1000);
            
            await task_delay;
            if (task_delay.IsCompleted)
            {
                Console.WriteLine($"Execute Query Completed! - {app_key} - {query_id}\n");
                query_states[query_id] = true;
            }

            return query_id;
        }

        public async Task<bool> GetQueryStateAsync(string query_id)
        {
            if (!query_states.ContainsKey(query_id))
                throw new Exception();
            await Task.Delay(100);
            return query_states[query_id];
        }
    }
}
