import { createCoreDbConnection } from '../utils/database';

export const internalTransferService = {
  // 获取内转数据（分页）
  async getInternalTransferData(startDate?: string, endDate?: string, page: number = 1, pageSize: number = 10) {
    // 参数验证
    const validPage = Math.max(1, parseInt(page.toString()));
    const validPageSize = Math.min(Math.max(1, parseInt(pageSize.toString())), 100); // 限制最大页面大小为100
    
    // 确定日期范围
    const defaultStartDate = startDate || 'DATE_SUB(CURDATE(), INTERVAL 30 DAY)';
    const defaultEndDate = endDate || 'CURDATE()';
    
    try {
      // 构建优化的SQL查询
      let sql = `
        SELECT 
          DATE_FORMAT(date_series.date_col, '%Y-%m-%d') AS query_date,
          
          -- 1. 注册人数
          COALESCE(register_stats.register_count, 0) AS register_count,
          
          -- 2. 归因上报-Registration
          COALESCE(adjust_registration_stats.registration_count, 0) AS adjust_registration_count,
          
          -- 3. OCR全部识别完成人数
          COALESCE(ocr_stats.ocr_count, 0) AS real_name_auth_count,
          
          -- 4. 个人信息提交人数
          COALESCE(info_stats.info_count, 0) AS credit_info_count,
          
          -- 5. 推送总人数
          COALESCE(push_total_stats.push_total_count, 0) AS push_total_count,
          
          -- 6. 个人信息推送给合作伙伴人数
          COALESCE(upload_stats.upload_count, 0) AS info_push_count,
          
          -- 6. 获取授信成功人数
          COALESCE(credit_stats.credit_count, 0) AS credit_success_count,
          
          -- 7. 提交贷款人数（所有状态）
          COALESCE(loan_submit_stats.loan_count, 0) AS loan_success_count,
          
          -- 8. 借款成功人数（status=1）
          COALESCE(loan_approved_stats.loan_approved_count, 0) AS loan_approved_count,
          
          -- 9. 已还款人数（repayment_status=1）
          COALESCE(loan_repaid_stats.loan_repaid_count, 0) AS loan_repaid_count

        FROM (
            -- 根据时间范围生成日期序列（MySQL 5.7兼容）
            SELECT DATE_ADD('${defaultStartDate}', INTERVAL (a.a + (10 * b.a)) DAY) AS date_col
            FROM (SELECT 0 AS a UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) AS a
            CROSS JOIN (SELECT 0 AS a UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) AS b
            WHERE DATE_ADD('${defaultStartDate}', INTERVAL (a.a + (10 * b.a)) DAY) <= '${defaultEndDate}'
        ) AS date_series

        LEFT JOIN (
            -- 注册人数统计
            SELECT 
                DATE(request_time) AS date_col,
                COUNT(DISTINCT user_id) AS register_count
            FROM user_login_record 
            WHERE is_new_user = 1
            GROUP BY DATE(request_time)
        ) register_stats ON register_stats.date_col = date_series.date_col

        LEFT JOIN (
            -- 归因上报-Registration统计
            SELECT 
                DATE(created_at) AS date_col,
                COUNT(DISTINCT user_id) AS registration_count
            FROM adjust_event_record 
            WHERE event_name = 'Registration' AND status = 1
            GROUP BY DATE(created_at)
        ) adjust_registration_stats ON adjust_registration_stats.date_col = date_series.date_col

        LEFT JOIN (
            -- OCR全部识别完成人数统计（从user_ocr_record表查询首次完成face-recognition事件的去重用户）
            SELECT 
                DATE(created_at) AS date_col,
                COUNT(DISTINCT user_id) AS ocr_count
                FROM user_info
                WHERE face_vs_ktp_score >= 60 and liveness_score >= 60
            GROUP BY DATE(created_at)
        ) ocr_stats ON ocr_stats.date_col = date_series.date_col

        LEFT JOIN (
            -- 个人信息提交人数统计（按verification_success_at字段统计认证成功的日期）
            SELECT 
                DATE(created_at) AS date_col,
               COUNT(DISTINCT(user_id)) AS info_count
            FROM user_partner_bank_record
            where status = 1
            GROUP BY DATE(created_at)
        ) info_stats ON info_stats.date_col = date_series.date_col

        LEFT JOIN (
            -- 推送总人数统计
          SELECT
          DATE(upload_all.created_at) AS date_col,
          COUNT(DISTINCT upload_all.user_id) AS push_total_count
          FROM (
          SELECT
          user_id,
          MIN(created_at) AS created_at
          FROM user_upload_records
          GROUP BY (user_id)
          ) AS upload_all
          GROUP BY DATE(upload_all.created_at)
          ) push_total_stats ON push_total_stats.date_col = date_series.date_col

        LEFT JOIN (
          SELECT
          DATE(upload.created_at) AS date_col,
          COUNT(DISTINCT upload.user_id) AS upload_count
          FROM (
          SELECT
          user_id,
          MIN(created_at) AS created_at
          FROM user_upload_records
          WHERE status = "success"
          GROUP BY (user_id)
          ) AS upload
          GROUP BY DATE(upload.created_at)
          ) upload_stats ON upload_stats.date_col = date_series.date_col
            -- 个人信息推送给合作伙伴人数统计
--             SELECT
--           MIN(created_at) AS date_col,
--                 COUNT(DISTINCT user_id) AS upload_count
--             FROM user_upload_records
--             WHERE status = "success"
--             GROUP BY DATE(created_at)
--         ) upload_stats ON upload_stats.date_col = date_series.date_col

        LEFT JOIN (
            -- 授信成功人数统计
          SELECT
          DATE(credit.created_at) AS date_col,
          COUNT(DISTINCT credit.user_id) AS credit_count
          FROM (
          SELECT
          user_id,
          MIN(created_at) AS created_at
          FROM user_credit_record
          WHERE credit_status = 2
          GROUP BY user_id
          ) AS credit
          GROUP BY DATE(credit.created_at)
        ) credit_stats ON credit_stats.date_col = date_series.date_col

        LEFT JOIN (
            -- 提交贷款人数统计（所有状态）
            SELECT 
                DATE(created_at) AS date_col,
                COUNT(DISTINCT id) AS loan_count
            FROM user_loans
            GROUP BY DATE(created_at)
        ) loan_submit_stats ON loan_submit_stats.date_col = date_series.date_col

        LEFT JOIN (
            -- 借款成功人数统计（status=1）
          SELECT
          DATE(plan.created_at) AS date_col,
          COUNT(DISTINCT plan.order_no) AS loan_approved_count
          FROM (
          SELECT
          order_no,
          MIN(created_at) AS created_at
          FROM scheduled_repay_plan
          WHERE partner_order_status = 2
          GROUP BY order_no
          ) AS plan
          GROUP BY DATE(plan.created_at)
        ) loan_approved_stats ON loan_approved_stats.date_col = date_series.date_col

        LEFT JOIN (
            -- 已还款人数统计（假设partner_order_status=3表示已还款，请根据实际情况调整）
          SELECT
          DATE(plan.created_at) AS date_col,
          COUNT(DISTINCT plan.order_no) AS loan_repaid_count
          FROM (
          SELECT
          order_no,
          MIN(created_at) AS created_at
          FROM scheduled_repay_plan
          WHERE partner_order_status = 3
          GROUP BY order_no
          ) AS plan
          GROUP BY DATE(plan.created_at)
        ) loan_repaid_stats ON loan_repaid_stats.date_col = date_series.date_col

        WHERE date_series.date_col IS NOT NULL
        ORDER BY date_series.date_col DESC
        LIMIT ${validPageSize} OFFSET ${(validPage - 1) * validPageSize}
      `;

      console.log('执行SQL查询:', sql);
      
      const connection = await createCoreDbConnection();
      const [rows] = await connection.execute(sql);
      await connection.end();

      // 获取总数
      const countSql = `
        SELECT COUNT(*) as total
        FROM (
            -- 根据时间范围生成日期序列（MySQL 5.7兼容）
            SELECT DATE_ADD('${defaultStartDate}', INTERVAL (a.a + (10 * b.a)) DAY) AS date_col
            FROM (SELECT 0 AS a UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) AS a
            CROSS JOIN (SELECT 0 AS a UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) AS b
            WHERE DATE_ADD('${defaultStartDate}', INTERVAL (a.a + (10 * b.a)) DAY) <= '${defaultEndDate}'
        ) AS date_series
        WHERE date_series.date_col IS NOT NULL
      `;

      const countConnection = await createCoreDbConnection();
      const [countRows] = await countConnection.execute(countSql);
      await countConnection.end();

      const total = (countRows as any)[0]?.total || 0;
      const totalPages = Math.ceil(total / validPageSize);

      return {
        data: rows,
        pagination: {
          page: validPage,
          limit: validPageSize,
          total: total,
          totalPages: totalPages
        }
      };

    } catch (error) {
      console.error('获取内转数据失败:', error);
      throw error;
    }
  },

  // 获取内转图表数据（不分页）
  async getInternalTransferChartData(startDate?: string, endDate?: string) {
    // 确定日期范围
    const defaultStartDate = startDate || 'DATE_SUB(CURDATE(), INTERVAL 30 DAY)';
    const defaultEndDate = endDate || 'CURDATE()';
    
    try {
      // 构建优化的SQL查询
      let sql = `
        SELECT 
          DATE_FORMAT(date_series.date_col, '%Y-%m-%d') AS query_date,
          
          -- 1. 注册人数
          COALESCE(register_stats.register_count, 0) AS register_count,
          
          -- 2. 归因上报-Registration
          COALESCE(adjust_registration_stats.registration_count, 0) AS adjust_registration_count,
          
          -- 3. OCR全部识别完成人数
          COALESCE(ocr_stats.ocr_count, 0) AS real_name_auth_count,
          
          -- 4. 个人信息提交人数
          COALESCE(info_stats.info_count, 0) AS credit_info_count,
          
          -- 5. 推送总人数
          COALESCE(push_total_stats.push_total_count, 0) AS push_total_count,
          
          -- 6. 个人信息推送给合作伙伴人数
          COALESCE(upload_stats.upload_count, 0) AS info_push_count,
          
          -- 6. 获取授信成功人数
          COALESCE(credit_stats.credit_count, 0) AS credit_success_count,
          
          -- 7. 提交贷款人数（所有状态）
          COALESCE(loan_submit_stats.loan_count, 0) AS loan_success_count,
          
          -- 8. 借款成功人数（status=1）
          COALESCE(loan_approved_stats.loan_approved_count, 0) AS loan_approved_count,
          
          -- 9. 已还款人数（repayment_status=1）
          COALESCE(loan_repaid_stats.loan_repaid_count, 0) AS loan_repaid_count

        FROM (
            -- 根据时间范围生成日期序列（MySQL 5.7兼容）
            SELECT DATE_ADD('${defaultStartDate}', INTERVAL (a.a + (10 * b.a)) DAY) AS date_col
            FROM (SELECT 0 AS a UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) AS a
            CROSS JOIN (SELECT 0 AS a UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) AS b
            WHERE DATE_ADD('${defaultStartDate}', INTERVAL (a.a + (10 * b.a)) DAY) <= '${defaultEndDate}'
        ) AS date_series

        LEFT JOIN (
            -- 注册人数统计
            SELECT 
                DATE(request_time) AS date_col,
                COUNT(DISTINCT user_id) AS register_count
            FROM user_login_record 
            WHERE is_new_user = 1
            GROUP BY DATE(request_time)
        ) register_stats ON register_stats.date_col = date_series.date_col

        LEFT JOIN (
            -- 归因上报-Registration统计
            SELECT 
                DATE(created_at) AS date_col,
                COUNT(DISTINCT user_id) AS registration_count
            FROM adjust_event_record 
            WHERE event_name = 'Registration' AND status = 1
            GROUP BY DATE(created_at)
        ) adjust_registration_stats ON adjust_registration_stats.date_col = date_series.date_col

        LEFT JOIN (
            -- OCR全部识别完成人数统计（从user_ocr_record表查询首次完成face-recognition事件的去重用户）
            SELECT 
                DATE(first_completion.created_at) AS date_col,
                COUNT(DISTINCT first_completion.user_id) AS ocr_count
            FROM (
                SELECT 
                    user_id,
                    MIN(created_at) AS created_at
                FROM user_ocr_record
                WHERE event_name = 'face-recognition' 
                    AND recognition_status = 1
                GROUP BY user_id
            ) AS first_completion
            GROUP BY DATE(first_completion.created_at)
        ) ocr_stats ON ocr_stats.date_col = date_series.date_col

        LEFT JOIN (
            -- 个人信息提交人数统计（按verification_success_at字段统计认证成功的日期）
            SELECT 
                DATE(verification_success_at) AS date_col,
                COUNT(DISTINCT user_id) AS info_count
            FROM user_info 
            WHERE verification_success_at IS NOT NULL
            GROUP BY DATE(verification_success_at)
        ) info_stats ON info_stats.date_col = date_series.date_col

        LEFT JOIN (
            -- 个人信息推送给合作伙伴人数统计
            SELECT 
                DATE(created_at) AS date_col,
                COUNT(DISTINCT user_id) AS upload_count
            FROM user_upload_records 
            WHERE status = "success"
            GROUP BY DATE(created_at)
        ) upload_stats ON upload_stats.date_col = date_series.date_col

        LEFT JOIN (
            -- 授信成功人数统计
          SELECT
          DATE(credit.created_at) AS date_col,
          COUNT(DISTINCT credit.user_id) AS credit_count
          FROM (
          SELECT
          user_id,
          MIN(created_at) AS created_at
          FROM user_credit_record
          WHERE credit_status = 2
          GROUP BY user_id
          ) AS credit
          GROUP BY DATE(credit.created_at)
        ) credit_stats ON credit_stats.date_col = date_series.date_col

          LEFT JOIN (
          -- 提交贷款人数统计（所有状态）
          SELECT
          DATE(created_at) AS date_col,
          COUNT(DISTINCT order_no) AS loan_count
          FROM user_loans
          GROUP BY DATE(created_at)
          ) loan_submit_stats ON loan_submit_stats.date_col = date_series.date_col

          LEFT JOIN (
          -- 借款成功人数统计（status=1）
          SELECT
          DATE(plan.created_at) AS date_col,
          COUNT(DISTINCT plan.order_no) AS loan_approved_count
          FROM (
          SELECT
          order_no,
          MIN(created_at) AS created_at
          FROM scheduled_repay_plan
          WHERE partner_order_status = 2
          GROUP BY order_no
          ) AS plan
          GROUP BY DATE(plan.created_at)
          ) loan_approved_stats ON loan_approved_stats.date_col = date_series.date_col

          LEFT JOIN (
          -- 已还款人数统计（假设partner_order_status=3表示已还款，请根据实际情况调整）
          SELECT
          DATE(plan.created_at) AS date_col,
          COUNT(DISTINCT plan.order_no) AS loan_repaid_count
          FROM (
          SELECT
          order_no,
          MIN(created_at) AS created_at
          FROM scheduled_repay_plan
          WHERE partner_order_status = 3
          GROUP BY order_no
          ) AS plan
          GROUP BY DATE(plan.created_at)
          ) loan_repaid_stats ON loan_repaid_stats.date_col = date_series.date_col

        WHERE date_series.date_col IS NOT NULL
        ORDER BY date_series.date_col ASC
      `;

      console.log('执行图表SQL查询:', sql);
      
      const connection = await createCoreDbConnection();
      const [rows] = await connection.execute(sql);
      await connection.end();

      // 打印返回的数据用于调试
      console.log('图表查询返回的数据行数:', (rows as any).length);
      if ((rows as any).length > 0) {
        console.log('图表第一行数据:', (rows as any)[0]);
        console.log('图表数据字段名:', Object.keys((rows as any)[0]));
      }

      return rows;

    } catch (error) {
      console.error('获取内转图表数据失败:', error);
      throw error;
    }
  },

  // 获取某一天的详细数据（用户明细）
  async getInternalTransferDetails(date: string, type: string) {
    try {
      let sql = '';
      
      switch (type) {
        case 'register':
          // 注册人数明细（完全参考汇总逻辑，返回所有字段）
          sql = `
            SELECT *
            FROM user_login_record 
            WHERE is_new_user = 1 
              AND DATE(request_time) = '${date}'
            ORDER BY request_time DESC
            LIMIT 1000
          `;
          break;

        case 'real_name_auth':
          // OCR全部识别完成人数明细（完全参考汇总逻辑的子查询，关联用户登录表获取os_name）
          sql = `
            SELECT ocr.*, ulr.os_name
            FROM (
              SELECT 
                user_id,
                MIN(created_at) AS created_at
              FROM user_ocr_record
              WHERE event_name = 'face-recognition' 
                AND recognition_status = 1
              GROUP BY user_id
            ) AS first_completion
            INNER JOIN user_ocr_record ocr 
              ON ocr.user_id = first_completion.user_id 
              AND ocr.created_at = first_completion.created_at
            LEFT JOIN (
              SELECT ulr1.user_id, ulr1.os_name
              FROM user_login_record ulr1
              INNER JOIN (
                SELECT user_id, MIN(request_time) AS first_login_time
                FROM user_login_record
                WHERE user_id IS NOT NULL
                GROUP BY user_id
              ) AS first_login ON first_login.user_id = ulr1.user_id 
                AND first_login.first_login_time = ulr1.request_time
            ) AS ulr ON ulr.user_id = ocr.user_id
            WHERE DATE(first_completion.created_at) = '${date}'
            ORDER BY first_completion.created_at DESC
            LIMIT 1000
          `;
          break;

        case 'credit_info':
          // 个人信息提交人数明细（完全参考汇总逻辑，返回所有字段，关联用户登录表获取os_name）
          sql = `
            SELECT ui.*, ulr.os_name
            FROM user_info ui
            LEFT JOIN (
              SELECT ulr1.user_id, ulr1.os_name
              FROM user_login_record ulr1
              INNER JOIN (
                SELECT user_id, MIN(request_time) AS first_login_time
                FROM user_login_record
                WHERE user_id IS NOT NULL
                GROUP BY user_id
              ) AS first_login ON first_login.user_id = ulr1.user_id 
                AND first_login.first_login_time = ulr1.request_time
            ) AS ulr ON ulr.user_id = ui.user_id
            WHERE ui.verification_success_at IS NOT NULL
              AND DATE(ui.verification_success_at) = '${date}'
            ORDER BY ui.verification_success_at DESC
            LIMIT 1000
          `;
          break;

        case 'push_total':
          // 推送总人数明细（完全参考汇总逻辑的子查询，关联用户登录表获取os_name）
          sql = `
            SELECT upr.*, ulr.os_name
            FROM (
              SELECT 
                user_id,
                MIN(created_at) AS created_at
              FROM user_upload_records
              GROUP BY user_id
            ) AS first_push
            INNER JOIN user_upload_records upr 
              ON upr.user_id = first_push.user_id 
              AND upr.created_at = first_push.created_at
            LEFT JOIN (
              SELECT ulr1.user_id, ulr1.os_name
              FROM user_login_record ulr1
              INNER JOIN (
                SELECT user_id, MIN(request_time) AS first_login_time
                FROM user_login_record
                WHERE user_id IS NOT NULL
                GROUP BY user_id
              ) AS first_login ON first_login.user_id = ulr1.user_id 
                AND first_login.first_login_time = ulr1.request_time
            ) AS ulr ON ulr.user_id = upr.user_id
            WHERE DATE(first_push.created_at) = '${date}'
            ORDER BY first_push.created_at DESC
            LIMIT 1000
          `;
          break;

        case 'info_push':
          // 个人信息推送成功人数明细（完全参考汇总逻辑的子查询，关联用户登录表获取os_name）
          sql = `
            SELECT upr.*, ulr.os_name
            FROM (
              SELECT 
                user_id,
                MIN(created_at) AS created_at
              FROM user_upload_records
              WHERE status = 'success'
              GROUP BY user_id
            ) AS first_success
            INNER JOIN user_upload_records upr 
              ON upr.user_id = first_success.user_id 
              AND upr.created_at = first_success.created_at
            LEFT JOIN (
              SELECT ulr1.user_id, ulr1.os_name
              FROM user_login_record ulr1
              INNER JOIN (
                SELECT user_id, MIN(request_time) AS first_login_time
                FROM user_login_record
                WHERE user_id IS NOT NULL
                GROUP BY user_id
              ) AS first_login ON first_login.user_id = ulr1.user_id 
                AND first_login.first_login_time = ulr1.request_time
            ) AS ulr ON ulr.user_id = upr.user_id
            WHERE DATE(first_success.created_at) = '${date}'
            ORDER BY first_success.created_at DESC
            LIMIT 1000
          `;
          break;

        case 'credit_success':
          // 授信成功人数明细（完全参考汇总逻辑的子查询，关联用户登录表获取os_name）
          sql = `
            SELECT ucr.*, ulr.os_name
            FROM (
              SELECT
                user_id,
                MIN(created_at) AS created_at
              FROM user_credit_record
              WHERE credit_status = 2
              GROUP BY user_id
            ) AS credit
            INNER JOIN user_credit_record ucr 
              ON ucr.user_id = credit.user_id 
              AND ucr.created_at = credit.created_at
            LEFT JOIN (
              SELECT ulr1.user_id, ulr1.os_name
              FROM user_login_record ulr1
              INNER JOIN (
                SELECT user_id, MIN(request_time) AS first_login_time
                FROM user_login_record
                WHERE user_id IS NOT NULL
                GROUP BY user_id
              ) AS first_login ON first_login.user_id = ulr1.user_id 
                AND first_login.first_login_time = ulr1.request_time
            ) AS ulr ON ulr.user_id = ucr.user_id
            WHERE DATE(credit.created_at) = '${date}'
            ORDER BY credit.created_at DESC
            LIMIT 1000
          `;
          break;

        case 'loan_success':
          // 提交贷款明细（完全参考汇总逻辑，返回所有字段，关联用户登录表获取os_name）
          sql = `
            SELECT ul.*, ulr.os_name
            FROM user_loans ul
            LEFT JOIN (
              SELECT ulr1.user_id, ulr1.os_name
              FROM user_login_record ulr1
              INNER JOIN (
                SELECT user_id, MIN(request_time) AS first_login_time
                FROM user_login_record
                WHERE user_id IS NOT NULL
                GROUP BY user_id
              ) AS first_login ON first_login.user_id = ulr1.user_id 
                AND first_login.first_login_time = ulr1.request_time
            ) AS ulr ON ulr.user_id = ul.user_id
            WHERE DATE(ul.created_at) = '${date}'
            ORDER BY ul.created_at DESC
            LIMIT 1000
          `;
          break;

        case 'loan_approved':
          // 借款成功明细（完全参考汇总逻辑的子查询，关联用户登录表获取os_name）
          sql = `
            SELECT srp.*, ulr.os_name
            FROM (
              SELECT
                order_no,
                MIN(created_at) AS created_at
              FROM scheduled_repay_plan
              WHERE partner_order_status = 2
              GROUP BY order_no
            ) AS plan
            INNER JOIN scheduled_repay_plan srp 
              ON srp.order_no = plan.order_no 
              AND srp.created_at = plan.created_at
            LEFT JOIN user_loans ul ON ul.order_no = srp.order_no
            LEFT JOIN (
              SELECT ulr1.user_id, ulr1.os_name
              FROM user_login_record ulr1
              INNER JOIN (
                SELECT user_id, MIN(request_time) AS first_login_time
                FROM user_login_record
                WHERE user_id IS NOT NULL
                GROUP BY user_id
              ) AS first_login ON first_login.user_id = ulr1.user_id 
                AND first_login.first_login_time = ulr1.request_time
            ) AS ulr ON ulr.user_id = ul.user_id
            WHERE DATE(plan.created_at) = '${date}'
            ORDER BY plan.created_at DESC
            LIMIT 1000
          `;
          break;

        case 'loan_repaid':
          // 已还款明细（完全参考汇总逻辑的子查询，关联用户登录表获取os_name）
          sql = `
            SELECT srp.*, ulr.os_name
            FROM (
              SELECT
                order_no,
                MIN(created_at) AS created_at
              FROM scheduled_repay_plan
              WHERE partner_order_status = 3
              GROUP BY order_no
            ) AS plan
            INNER JOIN scheduled_repay_plan srp 
              ON srp.order_no = plan.order_no 
              AND srp.created_at = plan.created_at
            LEFT JOIN user_loans ul ON ul.order_no = srp.order_no
            LEFT JOIN (
              SELECT ulr1.user_id, ulr1.os_name
              FROM user_login_record ulr1
              INNER JOIN (
                SELECT user_id, MIN(request_time) AS first_login_time
                FROM user_login_record
                WHERE user_id IS NOT NULL
                GROUP BY user_id
              ) AS first_login ON first_login.user_id = ulr1.user_id 
                AND first_login.first_login_time = ulr1.request_time
            ) AS ulr ON ulr.user_id = ul.user_id
            WHERE DATE(plan.created_at) = '${date}'
            ORDER BY plan.created_at DESC
            LIMIT 1000
          `;
          break;

        default:
          throw new Error('无效的类型参数');
      }

      console.log('执行详情SQL查询:', sql);
      
      const connection = await createCoreDbConnection();
      const [rows] = await connection.execute(sql);
      await connection.end();

      return rows;

    } catch (error) {
      console.error('获取内转详细数据失败:', error);
      throw error;
    }
  }
};
