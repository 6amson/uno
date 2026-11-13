use crate::domain::models::PaymentState;
use crate::domain::errors::GatewayError;

/// The pure state machine. No I/O, no clock, no DB.
///
/// Legal transitions (per spec):
///   PENDING      -> AUTHORIZED   (authorize)
///   AUTHORIZED   -> CAPTURED     (capture)
///   CAPTURED     -> REFUNDED     (refund)
///   AUTHORIZED   -> VOIDED       (void)
///
/// Every other transition is rejected.
pub struct StateMachine;

impl StateMachine {
    /// Returns true if `transition` is a legal move.
    pub fn allowed(from: PaymentState, to: PaymentState) -> bool {
        matches!((from, to),
            (PaymentState::Pending, PaymentState::Authorized) |
            (PaymentState::Authorized, PaymentState::Captured) |
            (PaymentState::Captured, PaymentState::Refunded) |
            (PaymentState::Authorized, PaymentState::Voided)
        )
    }

    /// Applies a transition, returning an error if it is illegal.
    pub fn transition(from: PaymentState, to: PaymentState) -> Result<PaymentState, GatewayError> {
        if Self::allowed(from, to) {
            Ok(to)
        } else {
            Err(GatewayError::InvalidTransition {
                from: format!("{:?}", from),
                to: format!("{:?}", to),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn happy_path() {
        assert!(StateMachine::allowed(PaymentState::Pending, PaymentState::Authorized));
        assert!(StateMachine::allowed(PaymentState::Authorized, PaymentState::Captured));
        assert!(StateMachine::allowed(PaymentState::Captured, PaymentState::Refunded));
        assert!(StateMachine::allowed(PaymentState::Authorized, PaymentState::Voided));

        assert_eq!(
            StateMachine::transition(PaymentState::Pending, PaymentState::Authorized).unwrap(),
            PaymentState::Authorized
        );
    }

    #[test]
    fn reject_invalid_transitions() {
        // Cannot capture before authorize
        assert!(!StateMachine::allowed(PaymentState::Pending, PaymentState::Captured));
        // Cannot refund before capture
        assert!(!StateMachine::allowed(PaymentState::Authorized, PaymentState::Refunded));
        // Cannot void after capture
        assert!(!StateMachine::allowed(PaymentState::Captured, PaymentState::Voided));
        // Cannot go back to pending
        assert!(!StateMachine::allowed(PaymentState::Authorized, PaymentState::Pending));
        // Cannot self-transition
        assert!(!StateMachine::allowed(PaymentState::Captured, PaymentState::Captured));

        let err = StateMachine::transition(PaymentState::Pending, PaymentState::Captured).unwrap_err();
        match err {
            GatewayError::InvalidTransition { from, to } => {
                assert_eq!(from, "Pending");
                assert_eq!(to, "Captured");
            }
            other => panic!("expected InvalidTransition, got {:?}", other),
        }
    }

    #[test]
    fn terminal_states_have_no_outgoing_transitions() {
        assert!(!StateMachine::allowed(PaymentState::Refunded, PaymentState::Voided));
        assert!(!StateMachine::allowed(PaymentState::Voided, PaymentState::Refunded));
        assert!(!StateMachine::allowed(PaymentState::Refunded, PaymentState::Captured));
    }
}
