#include <stdexec/execution.hpp>

#include <tbb/parallel_for.h>
#include <tbb/task_arena.h>

namespace veeam {
template <typename ReceiverId> class tbb_operation;

class tbb_context {
public:
  template <typename ReceiverId> friend class tbb_operation;

  tbb_context(std::size_t nThreads) : arena(nThreads) {}
  tbb_context() : arena{} {}

  class scheduler {
  public:
    bool operator==(const scheduler&) const = default;

    class sender {
    public:
      using completion_signatures =
          stdexec::completion_signatures<stdexec::set_value_t(),
                                         stdexec::set_stopped_t()>;

    private:
      template <typename CPO>
      friend tbb_context::scheduler
      tag_invoke(stdexec::get_completion_scheduler_t<CPO>, sender s) noexcept {
        return s.context_.get_scheduler();
      }

      template <class Receiver>
      friend tbb_operation<stdexec::__x<std::decay_t<Receiver>>>
      tag_invoke(stdexec::connect_t, sender s, Receiver&& r) {
        return tbb_operation<stdexec::__x<std::decay_t<Receiver>>>{
            s.context_, (Receiver &&) r};
      }

      friend class tbb_context::scheduler;
      explicit sender(tbb_context& context) noexcept : context_(context) {}
      tbb_context& context_;
    };

    sender make_sender_() const { return sender{*context_}; }

    friend sender tag_invoke(stdexec::schedule_t, const scheduler& s) noexcept {
      return s.make_sender_();
    }

    tbb_context* get_context() const noexcept { return context_; }

  private:
    friend class tbb_context;
    tbb_context* context_;
    scheduler(tbb_context* context) : context_(context) {}
  };

  template <class Fun, class Shape, class... Args>
  requires stdexec::__callable<Fun, Shape, Args...>
  using bulk_non_throwing = stdexec::__bool<
      // If function invocation doesn't throw
      stdexec::__nothrow_callable<Fun, Shape, Args...>&&
      // and emplacing a tuple doesn't throw
      noexcept(stdexec::__decayed_tuple<Args...>(std::declval<Args>()...))
      // there's no need to advertise completion with `exception_ptr`
      >;

  template <class SenderId, class ReceiverId, std::integral Shape, class Fun>
  struct bulk_op_state;

  template <class SenderId, class ReceiverId, class Shape, class Fn,
            bool MayThrow>
  struct bulk_receiver {
    using Sender = stdexec::__t<SenderId>;
    using Receiver = stdexec::__t<ReceiverId>;

    bulk_op_state<SenderId, ReceiverId, Shape, Fn>& op_state_;

    template <class... As>
    friend void tag_invoke(stdexec::set_value_t, bulk_receiver&& self,
                           As&&... as) noexcept {
      using tuple_t = stdexec::__decayed_tuple<As...>;
      auto data = std::make_shared<tuple_t>((As &&) as...);
      self.op_state_.context_.arena.execute(
          [data, &op_state_ = self.op_state_] {
            oneapi::tbb::parallel_for(
                oneapi::tbb::blocked_range<Shape>(Shape{0}, op_state_.shape_),
                [data, fn = std::move(op_state_.fn_)](
                    const oneapi::tbb::blocked_range<Shape>& is) {
                  for (Shape i = is.begin(); i != is.end(); ++i) {
                    std::apply([i, fn](auto&&... as) { fn(i, as...); }, *data);
                  }
                });
            std::apply(
                [&op_state_](As&&... as) {
                  stdexec::set_value((Receiver &&) op_state_.receiver_,
                                     (As &&) as...);
                },
                *data);
          });
    }

    template <
        stdexec::__one_of<stdexec::set_error_t, stdexec::set_stopped_t> Tag,
        class... As>
    friend void tag_invoke(Tag tag, bulk_receiver&& self, As&&... as) noexcept {
      tag((Receiver &&) self.op_state_.receiver_, (As &&) as...);
    }

    friend auto tag_invoke(stdexec::get_env_t, const bulk_receiver& self)
        -> stdexec::env_of_t<Receiver> {
      return stdexec::get_env(self.op_state_.receiver_);
    }
  };

  template <class SenderId, class ReceiverId, std::integral Shape, class Fun>
  struct bulk_op_state {
    using Sender = stdexec::__t<SenderId>;
    using Receiver = stdexec::__t<ReceiverId>;

    static constexpr bool may_throw = !stdexec::__v<stdexec::__value_types_of_t<
        Sender, stdexec::env_of_t<Receiver>,
        stdexec::__mbind_front_q<bulk_non_throwing, Fun, Shape>,
        stdexec::__q<stdexec::__mand>>>;

    using bulk_rcvr =
        bulk_receiver<SenderId, ReceiverId, Shape, Fun, may_throw>;
    using inner_op_state = stdexec::connect_result_t<Sender, bulk_rcvr>;

    tbb_context& context_;
    Shape shape_;
    Receiver receiver_;
    Fun fn_;
    inner_op_state inner_op_;

    friend void tag_invoke(stdexec::start_t, bulk_op_state& op) noexcept {
      stdexec::start(op.inner_op_);
    }

    bulk_op_state(tbb_context& context, Shape shape, Fun fn, Sender&& sender,
                  Receiver receiver)
        : context_{context}, shape_{shape}, receiver_{(Receiver &&) receiver},
          fn_{(Fun &&) fn}, inner_op_{stdexec::connect((Sender &&) sender,
                                                       bulk_rcvr{*this})} {}
  };

  template <class SenderId, std::integral Shape, class FunId>
  struct bulk_sender {
    using Sender = stdexec::__t<SenderId>;
    using Fun = stdexec::__t<FunId>;

    tbb_context& context_;
    Sender sndr_;
    Shape shape_;
    Fun fun_;

    template <class Fun, class Sender, class Env>
    using with_error_invoke_t = stdexec::__if_c<
        stdexec::__v<stdexec::__value_types_of_t<
            Sender, Env,
            stdexec::__mbind_front_q<bulk_non_throwing, Fun, Shape>,
            stdexec::__q<stdexec::__mand>>>,
        stdexec::completion_signatures<>, stdexec::__with_exception_ptr>;

    template <class... Tys>
    using set_value_t = stdexec::completion_signatures<stdexec::set_value_t(
        std::decay_t<Tys>...)>;

    template <class Self, class Env>
    using completion_signatures = stdexec::__make_completion_signatures<
        stdexec::__copy_cvref_t<Self, Sender>, Env,
        with_error_invoke_t<Fun, stdexec::__copy_cvref_t<Self, Sender>, Env>,
        stdexec::__q<set_value_t>>;

    template <class Self, class Receiver>
    using bulk_op_state_t =
        bulk_op_state<stdexec::__x<stdexec::__copy_cvref_t<Self, Sender>>,
                      stdexec::__x<std::remove_cvref_t<Receiver>>, Shape, Fun>;

    template <stdexec::__decays_to<bulk_sender> Self,
              stdexec::receiver Receiver>
    requires stdexec::receiver_of<
        Receiver, completion_signatures<Self, stdexec::env_of_t<Receiver>>>
    friend bulk_op_state_t<Self, Receiver>
    tag_invoke(stdexec::connect_t, Self&& self, Receiver&& rcvr) noexcept(
        std::is_nothrow_constructible_v<bulk_op_state_t<Self, Receiver>,
                                        tbb_context&, Shape, Fun, Sender,
                                        Receiver>) {
      return bulk_op_state_t<Self, Receiver>{self.context_, self.shape_,
                                             self.fun_, ((Self &&) self).sndr_,
                                             (Receiver &&) rcvr};
    }

    template <stdexec::__decays_to<bulk_sender> Self, class Env>
    friend auto tag_invoke(stdexec::get_completion_signatures_t, Self&&, Env)
        -> stdexec::dependent_completion_signatures<Env>;

    template <stdexec::__decays_to<bulk_sender> Self, class Env>
    friend auto tag_invoke(stdexec::get_completion_signatures_t, Self&&, Env)
        -> completion_signatures<Self, Env>
    requires true;

    template <stdexec::tag_category<stdexec::forwarding_sender_query> Tag,
              class... As>
    requires stdexec::__callable<Tag, const Sender&, As...>
    friend auto
    tag_invoke(Tag tag, const bulk_sender& self, As&&... as) noexcept(
        stdexec::__nothrow_callable<Tag, const Sender&, As...>)
        -> stdexec::__call_result_if_t<
            stdexec::tag_category<Tag, stdexec::forwarding_sender_query>, Tag,
            const Sender&, As...> {
      return ((Tag &&) tag)(self.sndr_, (As &&) as...);
    }
  };

  template <stdexec::sender Sender, std::integral Shape, class Fun>
  using bulk_sender_t =
      bulk_sender<stdexec::__x<std::remove_cvref_t<Sender>>, Shape,
                  stdexec::__x<std::remove_cvref_t<Fun>>>;

  template <stdexec::sender S, std::integral Shape, class Fn>
  friend bulk_sender_t<S, Shape, Fn> tag_invoke(stdexec::bulk_t,
                                                const scheduler& sch, S&& sndr,
                                                Shape shape, Fn fun) noexcept {
    return bulk_sender_t<S, Shape, Fn>{*sch.get_context(), (S &&) sndr, shape,
                                       (Fn &&) fun};
  }

  scheduler get_scheduler() { return scheduler{this}; }

public:
  tbb::task_arena arena{};
};

template <typename ReceiverId> class tbb_operation {
public:
  using Receiver = stdexec::__t<ReceiverId>;
  tbb_operation(tbb_context& context, Receiver receiver)
      : context_(context), receiver_(std::move(receiver)) {}

private:
  friend void tag_invoke(stdexec::start_t, tbb_operation& op) noexcept {
    op.context_.arena.enqueue(
        [&op] { stdexec::set_value((Receiver &&) op.receiver_); });
  }

  tbb_context& context_;
  Receiver receiver_;
};
} // namespace psi